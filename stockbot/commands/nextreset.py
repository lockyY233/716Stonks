from discord import Interaction, app_commands

from stockbot.config.runtime import get_app_config
from stockbot.db import get_state_value


def _current_tick() -> int:
    raw = get_state_value("last_tick")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def setup_nextreset(tree: app_commands.CommandTree) -> None:
    @tree.command(name="nextreset", description="Show time until the next trading-limit reset.")
    async def nextreset(interaction: Interaction) -> None:
        period = int(get_app_config("TRADING_LIMITS_PERIOD"))
        tick_interval = int(get_app_config("TICK_INTERVAL"))
        limit = int(get_app_config("TRADING_LIMITS"))

        if period <= 0:
            await interaction.response.send_message(
                "Trading limits are misconfigured: `TRADING_LIMITS_PERIOD` must be > 0.",
                ephemeral=True,
            )
            return

        tick = _current_tick()
        ticks_remaining = period - (tick % period)
        if ticks_remaining <= 0:
            ticks_remaining = period

        seconds_remaining = ticks_remaining * max(1, tick_interval)
        minutes_remaining = seconds_remaining / 60.0
        period_minutes = (period * max(1, tick_interval)) / 60.0

        if limit <= 0:
            suffix = "Trading limits are currently disabled."
        else:
            suffix = f"Current limit: {limit} shares per {period_minutes:.2f} minutes."

        await interaction.response.send_message(
            (
                f"Next trading-limit reset in **{minutes_remaining:.2f} minutes** "
                f"({ticks_remaining} ticks / {seconds_remaining} seconds).\n{suffix}"
            ),
            ephemeral=True,
        )
