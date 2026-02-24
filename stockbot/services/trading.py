from datetime import datetime, timezone

from discord import Interaction

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE
from stockbot.config.runtime import get_app_config
from stockbot.db import (
    add_action_history,
    get_commodity,
    get_company,
    get_state_value,
    get_user,
    get_user_commodities,
    get_user_shares,
    recalc_user_networth,
    set_holding,
    set_state_value,
    update_company_trade_volume,
    update_user_networth,
    update_user_bank,
    upsert_user_commodity,
    upsert_holding,
)
from stockbot.services.perks import check_and_announce_perk_activations, evaluate_user_perks


def _normalize_owner_ids(raw: object) -> list[int]:
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    seen: set[int] = set()
    for item in raw:
        try:
            uid = int(item)
        except (TypeError, ValueError):
            continue
        if uid <= 0 or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


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
    limit = int(get_app_config("TRADING_LIMITS"))
    period = int(get_app_config("TRADING_LIMITS_PERIOD"))
    tick_interval = max(1, int(get_app_config("TICK_INTERVAL")))
    if limit <= 0:
        return True, ""
    if period <= 0:
        return False, "Trading limits misconfigured: TRADING_LIMITS_PERIOD must be > 0."

    evaluated = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=limit,
        base_networth=0.0,
    )
    limit = int(evaluated["final"]["trade_limits"])
    if limit <= 0:
        period_minutes = (period * tick_interval) / 60.0
        return (
            False,
            (
                f"Trade limit reached for this period. Remaining: 0 shares "
                f"(limit {limit} per {period_minutes:.2f} minutes)."
            ),
        )

    tick = _get_current_tick()
    bucket = tick // period
    key = _limit_bucket_key(guild_id, user_id, bucket)
    used_raw = get_state_value(key)
    used = int(float(used_raw)) if used_raw is not None else 0
    if used + shares > limit:
        remaining = max(0, limit - used)
        period_minutes = (period * tick_interval) / 60.0
        return (
            False,
            (
                f"Trade limit reached for this period. Remaining: {remaining} shares "
                f"(limit {limit} per {period_minutes:.2f} minutes)."
            ),
        )

    set_state_value(key, str(used + shares))
    return True, ""


def _current_limit_status(guild_id: int, user_id: int) -> tuple[bool, int, int, float, int]:
    base_limit = int(get_app_config("TRADING_LIMITS"))
    period = int(get_app_config("TRADING_LIMITS_PERIOD"))
    tick_interval = max(1, int(get_app_config("TICK_INTERVAL")))
    period_minutes = (period * tick_interval) / 60.0 if period > 0 else 0.0
    if base_limit <= 0 or period <= 0:
        return False, base_limit, 0, period_minutes, 0
    evaluated = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=base_limit,
        base_networth=0.0,
    )
    limit = int(evaluated["final"]["trade_limits"])
    if limit <= 0:
        return True, 0, 0, period_minutes, 0

    tick = _get_current_tick()
    bucket = tick // period
    key = _limit_bucket_key(guild_id, user_id, bucket)
    used_raw = get_state_value(key)
    used = int(float(used_raw)) if used_raw is not None else 0
    remaining = max(0, limit - used)
    return True, limit, used, period_minutes, remaining


def _commodity_limit_status(guild_id: int, user_id: int) -> tuple[bool, int, int, int]:
    base_limit = int(get_app_config("COMMODITIES_LIMIT"))
    evaluated = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=0,
        base_networth=0.0,
        base_commodities_limit=base_limit,
        base_job_slots=1,
    )
    limit = int(evaluated["final"]["commodities_limit"])
    holdings = get_user_commodities(guild_id, user_id)
    used = sum(max(0, int(row.get("quantity", 0))) for row in holdings)
    if limit <= 0:
        return False, limit, used, 0
    remaining = max(0, limit - used)
    return True, limit, used, remaining


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
    owner_user_ids = _normalize_owner_ids(company.get("owner_user_ids", []))
    if int(interaction.user.id) in owner_user_ids:
        return False, "Company owners cannot buy their own stock."

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
    add_action_history(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action_type="buy",
        target_type="stock",
        target_symbol=symbol_upper,
        quantity=float(shares),
        unit_price=price,
        total_amount=total_cost,
        details="",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    owner_fee_rate_pct = max(0.0, min(100.0, float(get_app_config("OWNER_BUY_FEE_RATE"))))
    owner_fee = 0.0
    if owner_user_ids and owner_fee_rate_pct > 0:
        total_owner_fee = round(total_cost * (owner_fee_rate_pct / 100.0), 2)
        if total_owner_fee > 0:
            eligible_owner_ids: list[int] = []
            for owner_id in owner_user_ids:
                owner_user = get_user(interaction.guild.id, owner_id)
                if owner_user is not None:
                    eligible_owner_ids.append(owner_id)
            if eligible_owner_ids:
                cents = int(round(total_owner_fee * 100))
                count = len(eligible_owner_ids)
                base_cents = cents // count
                rem = cents % count
                splits = [base_cents + (1 if i < rem else 0) for i in range(count)]
                for idx, owner_id in enumerate(eligible_owner_ids):
                    part = splits[idx] / 100.0
                    if part <= 0:
                        continue
                    owner_user = get_user(interaction.guild.id, owner_id)
                    if owner_user is None:
                        continue
                    owner_fee += part
                    update_user_bank(
                        interaction.guild.id,
                        owner_id,
                        float(owner_user.get("bank", 0.0)) + part,
                    )
                    add_action_history(
                        guild_id=interaction.guild.id,
                        user_id=owner_id,
                        action_type="owner_payout",
                        target_type="stock",
                        target_symbol=symbol_upper,
                        quantity=float(shares),
                        unit_price=part / max(1, shares),
                        total_amount=part,
                        details=f"source=buy;buyer={interaction.user.id};rate={owner_fee_rate_pct:.2f}%;owners={count}",
                        created_at=datetime.now(timezone.utc).isoformat(),
                    )
                owner_fee = round(owner_fee, 2)
    limits_enabled, limit, _used, period_minutes, remaining = _current_limit_status(
        interaction.guild.id,
        interaction.user.id,
    )
    if limits_enabled:
        limit_display = f"{remaining}/{limit} shares remaining this {period_minutes:.2f}-minute window."
    else:
        limit_display = "Disabled."

    fee_ratio_pct = max(0.0, float(get_app_config("TRADING_FEES")))
    fee_text = (
        f"Transaction fee: {fee_ratio_pct:.2f}% on realized sell profit only (buy fee: $0.00)."
    )
    return (
        True,
        (
            f"Bought {shares} shares of {symbol_upper} for \n"
            f"==========\n"
            f"💰**${total_cost:.2f}**.\n"
            f"==========\n"
            f"- Trading limit: "
            f"**{limit_display}**\n"
            f"- Transaction fee: "
            f"{fee_ratio_pct:.2f}% => **$0.00** (buy fee disabled; applies on realized sell profit).\n"
            f"- Owner payout: "
            f"{owner_fee_rate_pct:.2f}% => **${owner_fee:.2f}**"
        ),
    )


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
    owner_user_ids = _normalize_owner_ids(company.get("owner_user_ids", []))
    if int(interaction.user.id) in owner_user_ids:
        return False, "Company owners cannot sell their own stock."

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
    fee_ratio = max(0.0, float(get_app_config("TRADING_FEES"))) / 100.0
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
    add_action_history(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action_type="sell",
        target_type="stock",
        target_symbol=symbol_upper,
        quantity=float(shares),
        unit_price=price,
        total_amount=net_gain,
        details=(f"gross={gross_gain:.2f};fee={fee:.2f}" if fee > 0 else ""),
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    limits_enabled, limit, _used, period_minutes, remaining = _current_limit_status(
        interaction.guild.id,
        interaction.user.id,
    )
    if limits_enabled:
        limit_display = f"{remaining}/{limit} shares remaining this {period_minutes:.2f}-minute window."
    else:
        limit_display = "Disabled."

    fee_pct = fee_ratio * 100.0
    realized_pl_before_fee = gross_gain - cost_removed
    realized_pl_after_fee = net_gain - cost_removed
    return (
        True,
        (
            f"Sold {shares} shares of {symbol_upper} for \n"
            f"**${net_gain:.2f}** net (gross ${gross_gain:.2f}).\n"
            f"- Avg buy price: \n"
            f"**${avg_cost:.2f}/share**\n"
            f"- Realized P/L (est.): \n"
            f"**${realized_pl_after_fee:+.2f}** "
            f"(before fee ${realized_pl_before_fee:+.2f})\n"
            f"- Trading limit: \n"
            f"{limit_display}\n"
            f"- Transaction fee: \n"
            f"{fee_pct:.2f}% => **${fee:.2f}**."
        ),
    )


async def perform_buy_commodity(
    interaction: Interaction,
    commodity_name: str,
    quantity: int,
) -> tuple[bool, str]:
    if interaction.guild is None:
        return False, "Please use this command in a server."

    if quantity <= 0:
        return False, "Quantity must be a positive integer."

    user = get_user(interaction.guild.id, interaction.user.id)
    if user is None:
        return False, REGISTER_REQUIRED_MESSAGE

    commodity = get_commodity(interaction.guild.id, commodity_name)
    if commodity is None:
        return False, f"Commodity `{commodity_name}` not found."
    if int(commodity.get("enabled", 1) or 0) != 1:
        return False, f"Commodity `{commodity_name}` is currently disabled."

    enabled, limit, _used, remaining = _commodity_limit_status(
        interaction.guild.id,
        interaction.user.id,
    )
    if enabled and quantity > remaining:
        return (
            False,
            f"Commodity limit reached. Remaining slots: {remaining}/{limit} units.",
        )

    price = float(commodity["price"])
    total_cost = round(price * quantity, 2)
    if float(user["bank"]) < total_cost:
        return False, f"Not enough funds. Need ${total_cost:.2f}, you have ${float(user['bank']):.2f}."

    new_bank = float(user["bank"]) - total_cost
    update_user_bank(interaction.guild.id, interaction.user.id, new_bank)
    upsert_user_commodity(
        interaction.guild.id,
        interaction.user.id,
        str(commodity["name"]),
        quantity,
    )
    new_networth = float(user.get("networth", 0.0)) + total_cost
    update_user_networth(interaction.guild.id, interaction.user.id, new_networth)
    add_action_history(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action_type="buy",
        target_type="commodity",
        target_symbol=str(commodity["name"]),
        quantity=float(quantity),
        unit_price=price,
        total_amount=total_cost,
        details="",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    await check_and_announce_perk_activations(interaction)
    return (
        True,
        f"Bought {quantity}x {commodity['name']} for ${total_cost:.2f}. "
        f"Commodity networth now ${new_networth:.2f}.",
    )


async def perform_pawn_commodity(
    interaction: Interaction,
    commodity_name: str,
    quantity: int,
) -> tuple[bool, str]:
    if interaction.guild is None:
        return False, "Please use this command in a server."

    if quantity <= 0:
        return False, "Quantity must be a positive integer."

    user = get_user(interaction.guild.id, interaction.user.id)
    if user is None:
        return False, REGISTER_REQUIRED_MESSAGE

    commodity = get_commodity(interaction.guild.id, commodity_name)
    if commodity is None:
        return False, f"Commodity `{commodity_name}` not found."

    holdings = get_user_commodities(interaction.guild.id, interaction.user.id)
    match = next(
        (row for row in holdings if str(row.get("name", "")).lower() == str(commodity.get("name", "")).lower()),
        None,
    )
    owned_qty = int(match.get("quantity", 0)) if match is not None else 0
    if owned_qty < quantity:
        return False, f"You only own {owned_qty}x {commodity['name']}."

    rate_pct = max(0.0, min(100.0, float(get_app_config("PAWN_SELL_RATE"))))
    rate = rate_pct / 100.0
    price = float(commodity["price"])
    gross_value = price * quantity
    payout = round(gross_value * rate, 2)

    new_bank = float(user["bank"]) + payout
    update_user_bank(interaction.guild.id, interaction.user.id, new_bank)
    upsert_user_commodity(
        interaction.guild.id,
        interaction.user.id,
        str(commodity["name"]),
        -quantity,
    )
    networth = recalc_user_networth(interaction.guild.id, interaction.user.id)

    add_action_history(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action_type="pawn",
        target_type="commodity",
        target_symbol=str(commodity["name"]),
        quantity=float(quantity),
        unit_price=price * rate,
        total_amount=payout,
        details=f"market_price={price:.2f};pawn_rate={rate_pct:.2f}%",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    await check_and_announce_perk_activations(interaction)

    return (
        True,
        (
            f"Pawned {quantity}x {commodity['name']} at {rate_pct:.2f}% for **${payout:.2f}** "
            f"(market value ${gross_value:.2f}).\n"
            f"New balance: ${new_bank:.2f} | Commodity networth: ${networth:.2f}"
        ),
    )


def preview_sell_transaction(
    guild_id: int,
    user_id: int,
    symbol: str,
    shares: int = 1,
) -> dict | None:
    if shares <= 0:
        return None

    user = get_user(guild_id, user_id)
    if user is None:
        return None

    company = get_company(guild_id, symbol.upper())
    if company is None:
        return None

    owned = get_user_shares(guild_id, user_id, symbol.upper())
    if owned < shares:
        return None

    price = float(company.get("current_price", company["base_price"]))
    gross_gain = float(price * shares)
    cost_basis_before = _get_cost_basis(guild_id, user_id, symbol.upper())
    if cost_basis_before <= 0 and owned > 0:
        cost_basis_before = float(owned) * price
    avg_cost = (cost_basis_before / float(owned)) if owned > 0 else price
    cost_removed = avg_cost * shares
    realized_profit = max(0.0, gross_gain - cost_removed)
    fee_ratio = max(0.0, float(get_app_config("TRADING_FEES"))) / 100.0
    fee = realized_profit * fee_ratio
    net_gain = max(0.0, gross_gain - fee)
    estimated_pl_before_fee = gross_gain - cost_removed
    estimated_pl_after_fee = net_gain - cost_removed
    return {
        "shares": int(shares),
        "unit_price": price,
        "avg_buy_price": avg_cost,
        "cost_basis_removed": cost_removed,
        "gross_gain": gross_gain,
        "fee_percent": fee_ratio * 100.0,
        "fee": fee,
        "net_gain": net_gain,
        "estimated_pl_before_fee": estimated_pl_before_fee,
        "estimated_pl_after_fee": estimated_pl_after_fee,
    }
