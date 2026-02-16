from datetime import datetime, timezone
import math
from zoneinfo import ZoneInfo

from stockbot.config.runtime import get_app_config
from stockbot.core.noise import (
    normalized_frequency_to_cycles_per_tick,
    perlin1d,
    stable_seed,
)
from stockbot.db import (
    add_price_history,
    get_companies,
    get_state_value,
    set_state_value,
    update_company_price,
    upsert_daily_close,
)


def process_tick(tick_index: int, guild_ids: list[int]) -> None:
    """Advance economy by one tick with base update and 1D Perlin drift noise."""
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    drift_noise_frequency = float(get_app_config("DRIFT_NOISE_FREQUENCY"))
    drift_noise_low_freq_ratio = float(get_app_config("DRIFT_NOISE_LOW_FREQ_RATIO"))
    drift_noise_gain = float(get_app_config("DRIFT_NOISE_GAIN"))
    drift_noise_low_gain = float(get_app_config("DRIFT_NOISE_LOW_GAIN"))
    market_close_hour = float(get_app_config("MARKET_CLOSE_HOUR"))
    display_timezone = str(get_app_config("DISPLAY_TIMEZONE"))
    fast_cycles_per_tick = normalized_frequency_to_cycles_per_tick(drift_noise_frequency)
    low_cycles_per_tick = fast_cycles_per_tick * max(0.0, drift_noise_low_freq_ratio)
    fast_gain = max(0.0, drift_noise_gain)
    low_gain = max(0.0, drift_noise_low_gain)
    try:
        display_tz = ZoneInfo(display_timezone)
    except Exception:
        display_tz = timezone.utc
    local_dt = now_dt.astimezone(display_tz)
    local_date = local_dt.strftime("%Y-%m-%d")
    local_hour = local_dt.hour + (local_dt.minute / 60.0) + (local_dt.second / 3600.0)
    for guild_id in guild_ids:
        companies = get_companies(guild_id)
        if not companies:
            continue

        for company in companies:
            symbol = company["symbol"]
            base_price = float(company["base_price"])
            # Slope is stored as percent-per-tick (e.g. 0.001 = +0.001% per tick).
            slope_pct = float(company["slope"])
            # Drift is stored as percent per tick (e.g. 1.5 means +/-1.5%).
            drift_pct = abs(float(company["drift"]))
            drift_ratio = drift_pct / 100.0
            liquidity = max(1.0, float(company.get("liquidity", 100.0)))
            impact_power = max(0.1, float(company.get("impact_power", 1.0)))
            pending_buy = float(company.get("pending_buy", 0.0))
            pending_sell = float(company.get("pending_sell", 0.0))
            starting_tick = int(company.get("starting_tick", 0))
            last_tick = int(company.get("last_tick", starting_tick))

            ticks_since_last = max(1, tick_index - last_tick)
            flow = (pending_buy - pending_sell) / liquidity
            nonlinear_impact = (abs(flow) ** impact_power) * (1.0 if flow >= 0 else -1.0)
            trend_factor = math.exp((slope_pct / 100.0) * ticks_since_last)
            trend_base_price = base_price * trend_factor
            next_base_price = max(0.01, trend_base_price + nonlinear_impact)
            seed = stable_seed(f"{guild_id}:{symbol}")
            seed_low = stable_seed(f"{guild_id}:{symbol}:low")
            phase_fast = (seed % 10007) / 10007.0
            phase_low = (seed_low % 10007) / 10007.0
            x_fast = (tick_index * fast_cycles_per_tick) + phase_fast
            x_low = (tick_index * low_cycles_per_tick) + phase_low
            noise_fast = perlin1d(x_fast, seed) * fast_gain
            noise_low = perlin1d(x_low, seed_low) * low_gain
            noise_unit = noise_fast + noise_low
            drift_noise = noise_unit * drift_ratio * next_base_price
            price = round(
                max(0.01, next_base_price + drift_noise),
                2,
            )

            update_company_price(
                guild_id=guild_id,
                symbol=symbol,
                base_price=next_base_price,
                slope=slope_pct,
                drift=drift_pct,
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
            if last_close_date != local_date and local_hour >= market_close_hour:
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
