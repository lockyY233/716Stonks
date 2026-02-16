from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from discord import Embed, Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config.runtime import get_app_config
from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME
from stockbot.db import get_connection, get_user
from stockbot.services.perks import evaluate_user_perks


def _owned_company_count(guild_id: int, user_id: int) -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM company_owners
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
    return int(row["c"]) if row is not None else 0


def _owner_payout_today(guild_id: int, user_id: int) -> float:
    tz_name = str(get_app_config("DISPLAY_TIMEZONE"))
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    now_local = datetime.now(timezone.utc).astimezone(tz)
    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_utc = day_start_local.astimezone(timezone.utc).isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(total_amount), 0.0) AS total
            FROM action_history
            WHERE guild_id = ?
              AND user_id = ?
              AND action_type = 'owner_payout'
              AND created_at >= ?
            """,
            (guild_id, user_id, day_start_utc),
        ).fetchone()
    return float(row["total"]) if row is not None else 0.0


def setup_income(tree: app_commands.CommandTree) -> None:
    @tree.command(name="income", description="Show income breakdown for a player.")
    async def income(interaction: Interaction, member: Member | None = None) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=False,
            )
            return

        target = member or interaction.user
        user = get_user(interaction.guild.id, target.id)
        if user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return

        rank = str(user.get("rank", DEFAULT_RANK))
        lookup = {k.lower(): float(v) for k, v in RANK_INCOME.items()}
        base_income = lookup.get(rank.lower(), float(RANK_INCOME.get(DEFAULT_RANK, 0.0)))
        base_trade_limits = int(get_app_config("TRADING_LIMITS"))
        result = evaluate_user_perks(
            guild_id=interaction.guild.id,
            user_id=target.id,
            base_income=base_income,
            base_trade_limits=base_trade_limits,
            base_networth=None,
        )
        final_income = float(result["final"]["income"])
        perk_bonus = final_income - base_income

        owned_count = _owned_company_count(interaction.guild.id, target.id)
        owner_fee_rate = float(get_app_config("OWNER_BUY_FEE_RATE"))
        owner_today = _owner_payout_today(interaction.guild.id, target.id)

        embed = Embed(
            title=f"Income Breakdown · {target.display_name}",
            description=(
                f"**${base_income:.2f}** (rank)"
                f" + **${perk_bonus:.2f}** (perks)"
                f" + **${owner_today:.2f}** (company today)"
            ),
        )
        embed.add_field(name="Rank", value=rank, inline=True)
        embed.add_field(name="Base Income (close)", value=f"${base_income:.2f}", inline=True)
        embed.add_field(name="Perk Delta (close)", value=f"${perk_bonus:+.2f}", inline=True)
        embed.add_field(name="Final Income (close)", value=f"**${final_income:.2f}**", inline=True)
        embed.add_field(
            name="Company Owner Income",
            value=(
                f"${owner_today:.2f} earned today\n"
                f"{owned_count} owned compan{'y' if owned_count == 1 else 'ies'}\n"
                f"Rate: {owner_fee_rate:.2f}% of non-owner buy volume"
            ),
            inline=True,
        )
        embed.add_field(name="Total (display)", value=f"${(final_income + owner_today):.2f}", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

