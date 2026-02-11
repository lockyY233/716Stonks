from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from random import uniform

from stockbot.config import DISPLAY_TIMEZONE, MARKET_CLOSE_HOUR
from stockbot.db import (
    add_price_history,
    get_companies,
    get_state_value,
    set_state_value,
    update_company_price,
    upsert_daily_close,
)


def process_tick(tick_index: int, guild_ids: list[int]) -> None:
    """Advance economy by one tick with simple base update and drift noise."""
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    try:
        display_tz = ZoneInfo(DISPLAY_TIMEZONE)
    except Exception:
        display_tz = timezone.utc
    local_dt = now_dt.astimezone(display_tz)
    local_date = local_dt.strftime("%Y-%m-%d")
    local_hour = local_dt.hour
    for guild_id in guild_ids:
        companies = get_companies(guild_id)
        if not companies:
            continue

        for company in companies:
            symbol = company["symbol"]
            base_price = float(company["base_price"])
            slope = float(company["slope"])
            drift_amount = abs(float(company["drift"]))
            liquidity = max(1.0, float(company.get("liquidity", 100.0)))
            pending_buy = float(company.get("pending_buy", 0.0))
            pending_sell = float(company.get("pending_sell", 0.0))
            starting_tick = int(company.get("starting_tick", 0))
            last_tick = int(company.get("last_tick", starting_tick))

            ticks_since_last = max(1, tick_index - last_tick)
            base_delta = (slope * ticks_since_last) + ((pending_buy - pending_sell) / liquidity)
            next_base_price = max(0.01, base_price + base_delta)
            drift_noise = uniform(-drift_amount, drift_amount)
            price = round(
                max(0.01, next_base_price + drift_noise),
                2,
            )

            update_company_price(
                guild_id=guild_id,
                symbol=symbol,
                base_price=next_base_price,
                slope=slope,
                drift=drift_amount,
                pending_buy=0.0,
                pending_sell=0.0,
                current_price=price,
                last_tick=tick_index,
                updated_at=now,
            )
            add_price_history(
                guild_id=guild_id,
                symbol=symbol,
                tick_index=tick_index,
                ts=now,
                price=price,
            )

            last_close_date = get_state_value(f"last_close_date:{guild_id}:{symbol}")
            if last_close_date != local_date and local_hour >= MARKET_CLOSE_HOUR:
                upsert_daily_close(
                    guild_id=guild_id,
                    symbol=symbol,
                    date=local_date,
                    close_price=price,
                )
                set_state_value(
                    f"last_close_date:{guild_id}:{symbol}",
                    local_date,
                )


def process_ticks(tick_indices: list[int], guild_ids: list[int]) -> None:
    for _ in tick_indices:
        process_tick(_, guild_ids)
