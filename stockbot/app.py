import asyncio
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import discord

from stockbot.commands import setup_commands
from stockbot.config.runtime import ensure_app_config_defaults, get_app_config
from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME, TOKEN
from stockbot.db import (
    add_action_history,
    apply_approved_loan,
    backfill_company_starting_ticks,
    create_database_backup,
    get_unprocessed_reviewed_bank_requests,
    get_close_news,
    get_top_users_by_networth,
    get_state_value,
    get_users,
    init_db,
    mark_bank_request_processed,
    recalc_all_networth,
    set_state_value,
    update_user_bank,
)
from stockbot.services.economy import process_ticks
from stockbot.services.announcements import (
    build_close_news_embed,
    build_close_ranking_lines,
    send_market_close_v2,
)
from stockbot.services.perks import apply_income_perks


class StockBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)
        self._synced = False
        self._tick_task: asyncio.Task | None = None

    async def setup_hook(self) -> None:
        setup_commands(self.tree)

    async def on_ready(self) -> None:
        if self._synced:
            return

        # Sync commands to every guild the bot is currently in.
        # this is to faster the command sync
        for guild in self.guilds:
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)

        self._synced = True
        if self._tick_task is None:
            self._tick_task = asyncio.create_task(self._tick_loop())

    async def _tick_loop(self) -> None:
        while not self.is_closed():
            tick_interval = max(1, int(get_app_config("TICK_INTERVAL")))
            last_tick_raw = get_state_value("last_tick")
            last_tick = int(last_tick_raw) if last_tick_raw is not None else 0
            next_tick = last_tick + 1

            tick_indices = [next_tick]
            guild_ids = [guild.id for guild in self.guilds]
            await asyncio.to_thread(process_ticks, tick_indices, guild_ids)
            set_state_value("last_tick", str(next_tick))
            set_state_value("last_tick_epoch", str(time.time()))
            await self._process_bank_requests()
            await self._maybe_send_market_close_announcement()

            await asyncio.sleep(tick_interval)

    async def _process_bank_requests(self) -> None:
        rows = await asyncio.to_thread(
            get_unprocessed_reviewed_bank_requests,
            limit=200,
        )
        if not rows:
            return

        announcement_channel_id = int(get_app_config("ANNOUNCEMENT_CHANNEL_ID"))
        now_iso = datetime.now(timezone.utc).isoformat()
        for row in rows:
            request_id = int(row["id"])
            guild_id = int(row["guild_id"])
            user_id = int(row["user_id"])
            status = str(row.get("status", ""))
            amount = float(row.get("amount", 0.0))
            request_type = str(row.get("request_type", "loan"))
            decision_reason = str(row.get("decision_reason", "")).strip()

            applied = False
            if status == "approved" and request_type == "loan":
                applied = await asyncio.to_thread(
                    apply_approved_loan,
                    guild_id=guild_id,
                    user_id=user_id,
                    amount=amount,
                )
                if applied:
                    await asyncio.to_thread(
                        add_action_history,
                        guild_id,
                        user_id,
                        "loan_approved",
                        "bank",
                        "LOAN",
                        amount,
                        1.0,
                        amount,
                        decision_reason,
                        now_iso,
                    )

            guild = self.get_guild(guild_id)
            if guild is not None:
                channel = await self._pick_announcement_channel(guild, announcement_channel_id)
                if channel is not None:
                    if status == "approved":
                        if applied:
                            message = (
                                f"<@{user_id}> your bank loan request was approved for `${amount:.2f}`."
                            )
                        else:
                            message = (
                                f"<@{user_id}> your bank loan request was approved,"
                                " but applying funds failed. Contact admin."
                            )
                    else:
                        message = f"<@{user_id}> your bank request was denied."
                    if decision_reason:
                        message += f"\nReason: {decision_reason}"
                    try:
                        await channel.send(message)
                    except Exception as exc:
                        print(
                            f"[announce] failed to send bank notification "
                            f"guild={guild.id} channel={getattr(channel, 'id', 'unknown')} "
                            f"request_id={request_id}: {exc}"
                        )

            await asyncio.to_thread(mark_bank_request_processed, request_id, now_iso)

    async def _maybe_send_market_close_announcement(self) -> None:
        display_timezone = str(get_app_config("DISPLAY_TIMEZONE"))
        market_close_hour = float(get_app_config("MARKET_CLOSE_HOUR"))
        stonkers_role_name = str(get_app_config("STONKERS_ROLE_NAME"))
        announcement_channel_id = int(get_app_config("ANNOUNCEMENT_CHANNEL_ID"))
        gm_id = int(get_app_config("GM_ID"))
        try:
            tz = ZoneInfo(display_timezone)
        except Exception:
            tz = timezone.utc
        now_local = datetime.now(timezone.utc).astimezone(tz)
        now_hour = now_local.hour + (now_local.minute / 60.0) + (now_local.second / 3600.0)
        if now_hour < market_close_hour:
            return

        local_date = now_local.strftime("%Y-%m-%d")
        await asyncio.to_thread(self._backup_database_if_needed, local_date)
        for guild in self.guilds:
            await asyncio.to_thread(self._apply_rank_income_for_guild, guild.id, local_date)

            state_key = f"networth_announce_date:{guild.id}"
            if get_state_value(state_key) == local_date:
                continue

            await asyncio.to_thread(recalc_all_networth, guild.id)
            top = await asyncio.to_thread(get_top_users_by_networth, guild.id, 50)
            if gm_id > 0:
                top = [row for row in top if int(row.get("user_id", 0)) != gm_id]
            top = top[:3]
            if not top:
                set_state_value(state_key, local_date)
                continue

            channel = await self._pick_announcement_channel(guild, announcement_channel_id)
            if channel is None:
                continue

            role = discord.utils.get(guild.roles, name=stonkers_role_name)
            role_mention = role.mention if role is not None else f"@{stonkers_role_name}"
            lines = build_close_ranking_lines(top)

            announcement_md = get_state_value(f"close_announcement_md:{guild.id}") or ""
            announced_ok = False
            try:
                sent_v2 = await send_market_close_v2(
                    channel=channel,
                    role_mention=role_mention,
                    announcement_md=announcement_md,
                    ranking_lines=lines,
                    content_suffix="market close!",
                )
                if not sent_v2:
                    print(
                        f"[announce] skipped market-close update (V2 required) "
                        f"guild={guild.id} channel={getattr(channel, 'id', 'unknown')}"
                    )
                    continue
                announced_ok = True
                news_rows = await asyncio.to_thread(get_close_news, guild.id, enabled_only=True)
                for row in news_rows:
                    news_embed = build_close_news_embed(row, preview=False)
                    await channel.send(embed=news_embed)
            except Exception as exc:
                print(
                    f"[announce] failed to send market-close update "
                    f"guild={guild.id} channel={getattr(channel, 'id', 'unknown')}: {exc}"
                )
            if announced_ok:
                set_state_value(state_key, local_date)

    def _backup_database_if_needed(self, local_date: str) -> None:
        state_key = "db_backup_date"
        if get_state_value(state_key) == local_date:
            return
        try:
            create_database_backup(prefix="market_close")
            set_state_value(state_key, local_date)
        except Exception as exc:
            print(f"[backup] failed to create market-close backup: {exc}")

    def _apply_rank_income_for_guild(self, guild_id: int, local_date: str) -> None:
        payout_state_key = f"rank_income_date:{guild_id}"
        if get_state_value(payout_state_key) == local_date:
            return

        users = get_users(guild_id)
        if not users:
            set_state_value(payout_state_key, local_date)
            return

        lookup = {key.lower(): float(value) for key, value in RANK_INCOME.items()}
        default_income = float(RANK_INCOME.get(DEFAULT_RANK, 0.0))
        for row in users:
            user_id = int(row["user_id"])
            bank = float(row.get("bank", 0.0))
            rank = str(row.get("rank", DEFAULT_RANK))
            base_income = lookup.get(rank.lower(), default_income)
            income, _perk_meta = apply_income_perks(guild_id, user_id, base_income)
            if income <= 0:
                continue
            update_user_bank(guild_id, user_id, bank + income)

        set_state_value(payout_state_key, local_date)
        return

    async def _pick_announcement_channel(
        self,
        guild: discord.Guild,
        preferred_channel_id: int = 0,
    ) -> discord.TextChannel | discord.Thread | None:
        # Explicit config: try this channel first.
        if preferred_channel_id > 0:
            channel = guild.get_channel(preferred_channel_id) or self.get_channel(preferred_channel_id)
            if channel is None:
                try:
                    channel = await guild.fetch_channel(preferred_channel_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    channel = None
            if isinstance(channel, (discord.TextChannel, discord.Thread)) and channel.guild.id == guild.id:
                return channel
            print(
                f"[announce] configured ANNOUNCEMENT_CHANNEL_ID={preferred_channel_id} "
                f"not usable in guild={guild.id}; falling back to auto-pick."
            )

        # Auto mode fallback.
        if guild.system_channel is not None:
            return guild.system_channel

        for channel in guild.text_channels:
            return channel
        return None


def run() -> None:
    init_db()
    ensure_app_config_defaults()
    backfill_company_starting_ticks()
    bot = StockBot()
    bot.run(TOKEN)
