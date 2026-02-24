from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable

from stockbot.config.settings import (
    DISPLAY_TIMEZONE,
    DRIFT_NOISE_FREQUENCY,
    DRIFT_NOISE_GAIN,
    DRIFT_NOISE_LOW_FREQ_RATIO,
    DRIFT_NOISE_LOW_GAIN,
    OWNER_BUY_FEE_RATE,
    SLOT_BET_MIN,
    SLOT_BET_MAX,
    SLOT_MAX_SPINS,
    SLOT_HOURLY_SPIN_LIMIT,
    SLOT_PAIR_PAYOUT_BASE,
    SLOT_TRIPLE_MULT_CHERRY,
    SLOT_TRIPLE_MULT_LEMON,
    SLOT_TRIPLE_MULT_WATERMELON,
    SLOT_TRIPLE_MULT_BELL,
    SLOT_TRIPLE_MULT_STAR,
    SLOT_TRIPLE_MULT_SEVEN,
    SLOT_TRIPLE_MULT_DIAMOND,
    GM_ID,
    ACTIVITY_APPLICATION_ID,
    MARKET_CLOSE_HOUR,
    ANNOUNCEMENT_CHANNEL_ID,
    ANNOUNCE_MENTION_ROLE,
    START_BALANCE,
    STONKERS_ROLE_NAME,
    TICK_INTERVAL,
    TRADING_FEES,
    COMMODITIES_LIMIT,
    PAWN_SELL_RATE,
    SHOP_RARITY_WEIGHTS,
    STONKERS_ROLE_INACTIVE,
    TRADING_LIMITS,
    TRADING_LIMITS_PERIOD,
    TREND_MULTIPLIER,
)
from stockbot.db.database import get_connection


@dataclass(frozen=True)
class AppConfigSpec:
    default: Any
    cast: Callable[[str], Any]
    description: str


APP_CONFIG_SPECS: dict[str, AppConfigSpec] = {
    "START_BALANCE": AppConfigSpec(
        default=float(START_BALANCE),
        cast=float,
        description="Starting cash for new users.",
    ),
    "TICK_INTERVAL": AppConfigSpec(
        default=int(TICK_INTERVAL),
        cast=int,
        description="Seconds between market ticks.",
    ),
    "TREND_MULTIPLIER": AppConfigSpec(
        default=float(TREND_MULTIPLIER),
        cast=float,
        description="Multiplier used by trend-related math.",
    ),
    "DISPLAY_TIMEZONE": AppConfigSpec(
        default=str(DISPLAY_TIMEZONE),
        cast=str,
        description="Timezone used for display and market-close checks.",
    ),
    "MARKET_CLOSE_HOUR": AppConfigSpec(
        default=float(MARKET_CLOSE_HOUR),
        cast=float,
        description="Local close time as decimal hour [0,24). Example: 21.5 means 21:30.",
    ),
    "STONKERS_ROLE_NAME": AppConfigSpec(
        default=str(STONKERS_ROLE_NAME),
        cast=str,
        description="Role granted on registration and pinged on close updates.",
    ),
    "STONKERS_ROLE_INACTIVE": AppConfigSpec(
        default=str(STONKERS_ROLE_INACTIVE),
        cast=str,
        description="Role assigned to users marked inactive (no activity in the last 24 hours).",
    ),
    "ANNOUNCEMENT_CHANNEL_ID": AppConfigSpec(
        default=int(ANNOUNCEMENT_CHANNEL_ID),
        cast=int,
        description="Discord channel ID used for announcements; 0 means auto-pick.",
    ),
    "ANNOUNCE_MENTION_ROLE": AppConfigSpec(
        default=int(ANNOUNCE_MENTION_ROLE),
        cast=int,
        description="1 to mention the configured role at close; 0 to disable mention.",
    ),
    "GM_ID": AppConfigSpec(
        default=int(GM_ID),
        cast=int,
        description="Discord user ID designated as game master; excluded from ranking leaderboards when >0.",
    ),
    "ACTIVITY_APPLICATION_ID": AppConfigSpec(
        default=int(ACTIVITY_APPLICATION_ID),
        cast=int,
        description="Discord embedded app application ID used by /activity to create launch invites.",
    ),
    "DRIFT_NOISE_FREQUENCY": AppConfigSpec(
        default=float(DRIFT_NOISE_FREQUENCY),
        cast=float,
        description="Normalized fast noise frequency [0,1].",
    ),
    "DRIFT_NOISE_GAIN": AppConfigSpec(
        default=float(DRIFT_NOISE_GAIN),
        cast=float,
        description="Fast noise gain multiplier.",
    ),
    "DRIFT_NOISE_LOW_FREQ_RATIO": AppConfigSpec(
        default=float(DRIFT_NOISE_LOW_FREQ_RATIO),
        cast=float,
        description="Low-band frequency ratio relative to fast frequency.",
    ),
    "DRIFT_NOISE_LOW_GAIN": AppConfigSpec(
        default=float(DRIFT_NOISE_LOW_GAIN),
        cast=float,
        description="Low-band noise gain multiplier.",
    ),
    "TRADING_LIMITS": AppConfigSpec(
        default=int(TRADING_LIMITS),
        cast=int,
        description="Max shares traded per period; <=0 disables limits.",
    ),
    "TRADING_LIMITS_PERIOD": AppConfigSpec(
        default=int(TRADING_LIMITS_PERIOD),
        cast=int,
        description="Tick window length for trading-limit reset.",
    ),
    "TRADING_FEES": AppConfigSpec(
        default=float(TRADING_FEES),
        cast=float,
        description="Sell fee percentage applied to realized profit.",
    ),
    "COMMODITIES_LIMIT": AppConfigSpec(
        default=int(COMMODITIES_LIMIT),
        cast=int,
        description="Max total commodity units a player can hold; <=0 disables.",
    ),
    "PAWN_SELL_RATE": AppConfigSpec(
        default=float(PAWN_SELL_RATE),
        cast=float,
        description="Pawn payout percentage when selling commodities to bank.",
    ),
    "SHOP_RARITY_WEIGHTS": AppConfigSpec(
        default=str(SHOP_RARITY_WEIGHTS),
        cast=str,
        description="JSON mapping of rarity->weight used for shop rotation.",
    ),
    "OWNER_BUY_FEE_RATE": AppConfigSpec(
        default=float(OWNER_BUY_FEE_RATE),
        cast=float,
        description="Owner payout percentage from non-owner buy transaction value.",
    ),
    "SLOT_BET_MIN": AppConfigSpec(
        default=float(SLOT_BET_MIN),
        cast=float,
        description="Minimum base bet per spin for /slot.",
    ),
    "SLOT_BET_MAX": AppConfigSpec(
        default=float(SLOT_BET_MAX),
        cast=float,
        description="Maximum base bet per spin for /slot before perk modifiers.",
    ),
    "SLOT_MAX_SPINS": AppConfigSpec(
        default=int(SLOT_MAX_SPINS),
        cast=int,
        description="Maximum spins per /slot command before perk modifiers.",
    ),
    "SLOT_HOURLY_SPIN_LIMIT": AppConfigSpec(
        default=int(SLOT_HOURLY_SPIN_LIMIT),
        cast=int,
        description="Base hourly /slot spin limit per player before perk modifiers.",
    ),
    "SLOT_PAIR_PAYOUT_BASE": AppConfigSpec(
        default=float(SLOT_PAIR_PAYOUT_BASE),
        cast=float,
        description="Base payout multiplier (of effective bet) for 2-of-a-kind slot results.",
    ),
    "SLOT_TRIPLE_MULT_CHERRY": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_CHERRY),
        cast=float,
        description="3x 🍒 payout multiplier.",
    ),
    "SLOT_TRIPLE_MULT_LEMON": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_LEMON),
        cast=float,
        description="3x 🍋 payout multiplier.",
    ),
    "SLOT_TRIPLE_MULT_WATERMELON": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_WATERMELON),
        cast=float,
        description="3x 🍉 payout multiplier.",
    ),
    "SLOT_TRIPLE_MULT_BELL": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_BELL),
        cast=float,
        description="3x 🔔 payout multiplier.",
    ),
    "SLOT_TRIPLE_MULT_STAR": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_STAR),
        cast=float,
        description="3x ⭐ payout multiplier.",
    ),
    "SLOT_TRIPLE_MULT_SEVEN": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_SEVEN),
        cast=float,
        description="3x 7️⃣ payout multiplier.",
    ),
    "SLOT_TRIPLE_MULT_DIAMOND": AppConfigSpec(
        default=float(SLOT_TRIPLE_MULT_DIAMOND),
        cast=float,
        description="3x 💎 payout multiplier.",
    ),
}


def _state_key(name: str) -> str:
    return f"config:{name}"


def _normalize(name: str, value: Any) -> Any:
    if name == "START_BALANCE":
        return max(0.0, float(value))
    if name == "TICK_INTERVAL":
        return max(1, int(value))
    if name == "TREND_MULTIPLIER":
        return float(value)
    if name == "DISPLAY_TIMEZONE":
        text = str(value).strip()
        return text or str(DISPLAY_TIMEZONE)
    if name == "MARKET_CLOSE_HOUR":
        close_hour = float(value)
        # Keep within one-day bounds while allowing sub-hour precision.
        return max(0.0, min(23.9997222222, close_hour))
    if name == "STONKERS_ROLE_NAME":
        text = str(value).strip()
        return text or str(STONKERS_ROLE_NAME)
    if name == "ANNOUNCEMENT_CHANNEL_ID":
        return max(0, int(value))
    if name == "STONKERS_ROLE_INACTIVE":
        text = str(value).strip()
        return text or str(STONKERS_ROLE_INACTIVE)
    if name == "ANNOUNCE_MENTION_ROLE":
        return 1 if int(value) > 0 else 0
    if name == "GM_ID":
        return max(0, int(value))
    if name == "ACTIVITY_APPLICATION_ID":
        return max(0, int(value))
    if name == "DRIFT_NOISE_FREQUENCY":
        return max(0.0, min(1.0, float(value)))
    if name == "DRIFT_NOISE_GAIN":
        return max(0.0, float(value))
    if name == "DRIFT_NOISE_LOW_FREQ_RATIO":
        return max(0.0, float(value))
    if name == "DRIFT_NOISE_LOW_GAIN":
        return max(0.0, float(value))
    if name == "TRADING_LIMITS":
        return int(value)
    if name == "TRADING_LIMITS_PERIOD":
        return max(1, int(value))
    if name == "TRADING_FEES":
        return max(0.0, float(value))
    if name == "COMMODITIES_LIMIT":
        return max(0, int(value))
    if name == "PAWN_SELL_RATE":
        return max(0.0, min(100.0, float(value)))
    if name == "SHOP_RARITY_WEIGHTS":
        text = str(value).strip()
        if not text:
            return str(SHOP_RARITY_WEIGHTS)
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return str(SHOP_RARITY_WEIGHTS)
        if not isinstance(parsed, dict):
            return str(SHOP_RARITY_WEIGHTS)
        normalized: dict[str, float] = {}
        for key, raw in parsed.items():
            k = str(key).strip().lower()
            if not k:
                continue
            try:
                normalized[k] = max(0.0, float(raw))
            except (TypeError, ValueError):
                continue
        if not normalized:
            return str(SHOP_RARITY_WEIGHTS)
        return json.dumps(normalized, separators=(",", ":"))
    if name == "OWNER_BUY_FEE_RATE":
        return max(0.0, min(100.0, float(value)))
    if name == "SLOT_BET_MIN":
        return max(0.01, float(value))
    if name == "SLOT_BET_MAX":
        return max(0.01, float(value))
    if name == "SLOT_MAX_SPINS":
        return max(1, int(value))
    if name == "SLOT_HOURLY_SPIN_LIMIT":
        return max(0, int(value))
    if name == "SLOT_PAIR_PAYOUT_BASE":
        return max(0.0, float(value))
    if name in {
        "SLOT_TRIPLE_MULT_CHERRY",
        "SLOT_TRIPLE_MULT_LEMON",
        "SLOT_TRIPLE_MULT_WATERMELON",
        "SLOT_TRIPLE_MULT_BELL",
        "SLOT_TRIPLE_MULT_STAR",
        "SLOT_TRIPLE_MULT_SEVEN",
        "SLOT_TRIPLE_MULT_DIAMOND",
    }:
        return max(0.0, float(value))
    return value


def _to_string(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def ensure_app_config_defaults() -> None:
    with get_connection() as conn:
        for name, spec in APP_CONFIG_SPECS.items():
            row = conn.execute(
                "SELECT value FROM app_state WHERE key = ?",
                (_state_key(name),),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO app_state (key, value)
                    VALUES (?, ?)
                    """,
                    (_state_key(name), _to_string(_normalize(name, spec.default))),
                )


def get_app_config(name: str) -> Any:
    spec = APP_CONFIG_SPECS.get(name)
    if spec is None:
        raise KeyError(f"Unknown app config: {name}")
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM app_state WHERE key = ?",
            (_state_key(name),),
        ).fetchone()
    if row is None:
        return _normalize(name, spec.default)
    raw = str(row["value"])
    try:
        parsed = spec.cast(raw)
    except (TypeError, ValueError):
        parsed = spec.default
    return _normalize(name, parsed)


def set_app_config(name: str, value: Any) -> Any:
    spec = APP_CONFIG_SPECS.get(name)
    if spec is None:
        raise KeyError(f"Unknown app config: {name}")
    normalized = _normalize(name, value)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (_state_key(name), _to_string(normalized)),
        )
        if name in {"MARKET_CLOSE_HOUR", "DISPLAY_TIMEZONE"}:
            conn.execute(
                "DELETE FROM app_state WHERE key LIKE 'close_event_sent:%'"
            )
            conn.execute(
                "DELETE FROM app_state WHERE key LIKE 'close_event_armed:%'"
            )
            conn.execute(
                "DELETE FROM app_state WHERE key LIKE 'rank_income_event:%'"
            )
    return normalized


def get_all_app_configs() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, spec in APP_CONFIG_SPECS.items():
        value = get_app_config(name)
        rows.append(
            {
                "name": name,
                "value": value,
                "default": _normalize(name, spec.default),
                "type": spec.cast.__name__,
                "description": spec.description,
            }
        )
    return rows
