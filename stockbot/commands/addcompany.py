from datetime import datetime, timezone
import time

from discord import Interaction, app_commands

from stockbot.config import TICK_INTERVAL
from stockbot.db import add_company, get_state_value


def setup_addcompany(tree: app_commands.CommandTree) -> None:
    @tree.command(name="addcompany", description="Admin: add a company.")
    @app_commands.checks.has_permissions(administrator=True)
    async def addcompany(
        interaction: Interaction,
        symbol: str,
        name: str,
        base_price: float,
        slope: float = 0.0,
        drift: float = 1.0,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        last_tick_raw = get_state_value("last_tick")
        current_tick = int(last_tick_raw) if last_tick_raw is not None else 0
        created = add_company(
            guild_id=interaction.guild.id,
            symbol=symbol.upper(),
            name=name,
            base_price=base_price,
            slope=slope,
            drift=drift,
            starting_tick=current_tick,
            current_price=base_price,
            last_tick=current_tick,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

        if created:
            await interaction.response.send_message(
                f"Company `{symbol.upper()}` added.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"Company `{symbol.upper()}` already exists.",
                ephemeral=True,
            )
