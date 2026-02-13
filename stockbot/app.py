import asyncio
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import discord

from stockbot.commands import setup_commands
from stockbot.config.runtime import ensure_app_config_defaults, get_app_config
from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME, TOKEN
from stockbot.db import (
    backfill_company_starting_ticks,
    create_database_backup,
    get_top_users_by_networth,
    get_state_value,
    get_users,
    init_db,
    recalc_all_networth,
    set_state_value,
    update_user_bank,
)
from stockbot.services.economy import process_ticks


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
            await self._maybe_send_market_close_announcement()

            await asyncio.sleep(tick_interval)

    async def _maybe_send_market_close_announcement(self) -> None:
        display_timezone = str(get_app_config("DISPLAY_TIMEZONE"))
        market_close_hour = int(get_app_config("MARKET_CLOSE_HOUR"))
        stonkers_role_name = str(get_app_config("STONKERS_ROLE_NAME"))
        try:
            tz = ZoneInfo(display_timezone)
        except Exception:
            tz = timezone.utc
        now_local = datetime.now(timezone.utc).astimezone(tz)
        if now_local.hour < market_close_hour:
            return

        local_date = now_local.strftime("%Y-%m-%d")
        await asyncio.to_thread(self._backup_database_if_needed, local_date)
        for guild in self.guilds:
            await asyncio.to_thread(self._apply_rank_income_for_guild, guild.id, local_date)

            state_key = f"networth_announce_date:{guild.id}"
            if get_state_value(state_key) == local_date:
                continue

            await asyncio.to_thread(recalc_all_networth, guild.id)
            top = await asyncio.to_thread(get_top_users_by_networth, guild.id, 3)
            if not top:
                set_state_value(state_key, local_date)
                continue

            channel = self._pick_announcement_channel(guild)
            if channel is None:
                continue

            role = discord.utils.get(guild.roles, name=stonkers_role_name)
            role_mention = role.mention if role is not None else f"@{stonkers_role_name}"
            lines = []
            for idx, row in enumerate(top, start=1):
                user_id = int(row["user_id"])
                networth = float(row.get("networth", 0.0))
                lines.append(f"**#{idx}** <@{user_id}> — Networth `${networth:.2f}`")

            embed = discord.Embed(
                title="Market Close: Commodity Networth Leaders",
                description="\n".join(lines),
            )
            await channel.send(content=f"{role_mention} market close update:", embed=embed)
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
            income = lookup.get(rank.lower(), default_income)
            if income <= 0:
                continue
            update_user_bank(guild_id, user_id, bank + income)

        set_state_value(payout_state_key, local_date)

    def _pick_announcement_channel(self, guild: discord.Guild) -> discord.abc.Messageable | None:
        if guild.system_channel is not None:
            perms = guild.system_channel.permissions_for(guild.me) if guild.me is not None else None
            if perms is not None and perms.send_messages:
                return guild.system_channel

        for channel in guild.text_channels:
            perms = channel.permissions_for(guild.me) if guild.me is not None else None
            if perms is not None and perms.send_messages:
                return channel
        return None


def run() -> None:
    init_db()
    ensure_app_config_defaults()
    backfill_company_starting_ticks()
    bot = StockBot()
    bot.run(TOKEN)
