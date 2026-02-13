from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from stockbot.config.settings import (
    DISPLAY_TIMEZONE,
    DRIFT_NOISE_FREQUENCY,
    DRIFT_NOISE_GAIN,
    DRIFT_NOISE_LOW_FREQ_RATIO,
    DRIFT_NOISE_LOW_GAIN,
    MARKET_CLOSE_HOUR,
    ANNOUNCEMENT_CHANNEL_ID,
    START_BALANCE,
    STONKERS_ROLE_NAME,
    TICK_INTERVAL,
    TRADING_FEES,
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
        default=int(MARKET_CLOSE_HOUR),
        cast=int,
        description="Local hour (0-23) used for daily market close logic.",
    ),
    "STONKERS_ROLE_NAME": AppConfigSpec(
        default=str(STONKERS_ROLE_NAME),
        cast=str,
        description="Role granted on registration and pinged on close updates.",
    ),
    "ANNOUNCEMENT_CHANNEL_ID": AppConfigSpec(
        default=int(ANNOUNCEMENT_CHANNEL_ID),
        cast=int,
        description="Discord channel ID used for announcements; 0 means auto-pick.",
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
        return max(0, min(23, int(value)))
    if name == "STONKERS_ROLE_NAME":
        text = str(value).strip()
        return text or str(STONKERS_ROLE_NAME)
    if name == "ANNOUNCEMENT_CHANNEL_ID":
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
