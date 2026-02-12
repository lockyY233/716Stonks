from datetime import datetime, timezone

from discord import Interaction

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE
from stockbot.config import TRADING_FEES, TRADING_LIMITS, TRADING_LIMITS_PERIOD
from stockbot.db import (
    get_company,
    get_state_value,
    get_user,
    get_user_shares,
    set_holding,
    set_state_value,
    update_company_trade_volume,
    update_user_bank,
    upsert_holding,
)


def _limit_bucket_key(guild_id: int, user_id: int, bucket_index: int) -> str:
    return f"trade_used:{guild_id}:{user_id}:{bucket_index}"


def _cost_basis_key(guild_id: int, user_id: int, symbol: str) -> str:
    return f"cost_basis:{guild_id}:{user_id}:{symbol}"


def _get_cost_basis(guild_id: int, user_id: int, symbol: str) -> float:
    raw = get_state_value(_cost_basis_key(guild_id, user_id, symbol))
    if raw is None:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def _set_cost_basis(guild_id: int, user_id: int, symbol: str, value: float) -> None:
    set_state_value(_cost_basis_key(guild_id, user_id, symbol), f"{max(0.0, float(value)):.6f}")


def _get_current_tick() -> int:
    raw = get_state_value("last_tick")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _enforce_and_consume_limit(guild_id: int, user_id: int, shares: int) -> tuple[bool, str]:
    limit = int(TRADING_LIMITS)
    period = int(TRADING_LIMITS_PERIOD)
    if limit <= 0:
        return True, ""
    if period <= 0:
        return False, "Trading limits misconfigured: TRADING_LIMITS_PERIOD must be > 0."

    tick = _get_current_tick()
    bucket = tick // period
    key = _limit_bucket_key(guild_id, user_id, bucket)
    used_raw = get_state_value(key)
    used = int(float(used_raw)) if used_raw is not None else 0
    if used + shares > limit:
        remaining = max(0, limit - used)
        return (
            False,
            f"Trade limit reached for this period. Remaining: {remaining} shares (limit {limit} per {period} ticks).",
        )

    set_state_value(key, str(used + shares))
    return True, ""


async def perform_buy(
    interaction: Interaction,
    symbol: str,
    shares: int,
) -> tuple[bool, str]:
    if interaction.guild is None:
        return False, "Please use this command in a server."

    if shares <= 0:
        return False, "Shares must be a positive integer."

    user = get_user(interaction.guild.id, interaction.user.id)
    if user is None:
        return False, REGISTER_REQUIRED_MESSAGE

    symbol_upper = symbol.upper()
    company = get_company(interaction.guild.id, symbol_upper)
    if company is None:
        return False, f"Company `{symbol_upper}` not found."

    price = float(company.get("current_price", company["base_price"]))
    total_cost = round(price * shares, 2)
    if user["bank"] < total_cost:
        return False, f"Not enough funds. Need ${total_cost:.2f}, you have ${float(user['bank']):.2f}."

    ok, limit_msg = _enforce_and_consume_limit(interaction.guild.id, interaction.user.id, shares)
    if not ok:
        return False, limit_msg

    owned_before = get_user_shares(interaction.guild.id, interaction.user.id, symbol_upper)
    cost_basis_before = _get_cost_basis(interaction.guild.id, interaction.user.id, symbol_upper)
    if cost_basis_before <= 0 and owned_before > 0:
        # Fallback for legacy users with no stored basis yet.
        cost_basis_before = float(owned_before) * price

    new_bank = float(user["bank"]) - total_cost
    update_user_bank(interaction.guild.id, interaction.user.id, new_bank)
    upsert_holding(interaction.guild.id, interaction.user.id, symbol_upper, shares)
    _set_cost_basis(
        interaction.guild.id,
        interaction.user.id,
        symbol_upper,
        cost_basis_before + total_cost,
    )
    update_company_trade_volume(
        interaction.guild.id,
        symbol_upper,
        float(shares),
        0.0,
        datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Bought {shares} shares of {symbol_upper} for ${total_cost:.2f}."


async def perform_sell(
    interaction: Interaction,
    symbol: str,
    shares: int,
) -> tuple[bool, str]:
    if interaction.guild is None:
        return False, "Please use this command in a server."

    if shares <= 0:
        return False, "Shares must be a positive integer."

    user = get_user(interaction.guild.id, interaction.user.id)
    if user is None:
        return False, REGISTER_REQUIRED_MESSAGE

    symbol_upper = symbol.upper()
    company = get_company(interaction.guild.id, symbol_upper)
    if company is None:
        return False, f"Company `{symbol_upper}` not found."

    owned = get_user_shares(interaction.guild.id, interaction.user.id, symbol_upper)
    if owned < shares:
        return False, f"You only own {owned} shares of {symbol_upper}."

    ok, limit_msg = _enforce_and_consume_limit(interaction.guild.id, interaction.user.id, shares)
    if not ok:
        return False, limit_msg

    price = float(company.get("current_price", company["base_price"]))
    gross_gain = float(price * shares)
    cost_basis_before = _get_cost_basis(interaction.guild.id, interaction.user.id, symbol_upper)
    if cost_basis_before <= 0 and owned > 0:
        # Fallback for legacy users with no stored basis yet.
        cost_basis_before = float(owned) * price
    avg_cost = (cost_basis_before / float(owned)) if owned > 0 else price
    cost_removed = avg_cost * shares
    realized_profit = max(0.0, gross_gain - cost_removed)
    fee_ratio = max(0.0, float(TRADING_FEES)) / 100.0
    fee = realized_profit * fee_ratio
    net_gain = round(max(0.0, gross_gain - fee), 2)

    new_bank = float(user["bank"]) + net_gain
    update_user_bank(interaction.guild.id, interaction.user.id, new_bank)
    remaining = owned - shares
    set_holding(interaction.guild.id, interaction.user.id, symbol_upper, remaining)
    new_cost_basis = max(0.0, cost_basis_before - cost_removed)
    if remaining <= 0:
        new_cost_basis = 0.0
    _set_cost_basis(interaction.guild.id, interaction.user.id, symbol_upper, new_cost_basis)

    update_company_trade_volume(
        interaction.guild.id,
        symbol_upper,
        0.0,
        float(shares),
        datetime.now(timezone.utc).isoformat(),
    )
    if fee > 0:
        return (
            True,
            f"Sold {shares} shares of {symbol_upper} for ${net_gain:.2f} net "
            f"(gross ${gross_gain:.2f}, fee ${fee:.2f} on profit).",
        )
    return True, f"Sold {shares} shares of {symbol_upper} for ${net_gain:.2f}."
