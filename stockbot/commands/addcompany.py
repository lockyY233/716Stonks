from datetime import datetime, timezone
from discord import Embed, Interaction, app_commands

from stockbot.commands.company_graphs import (
    build_player_impact_curve,
    build_price_projection_curve,
)
from stockbot.db import add_company, get_state_value

def setup_addcompany(tree: app_commands.CommandTree) -> None:
    @tree.command(name="addcompany", description="Admin: add a company.")
    @app_commands.describe(
        symbol="Ticker symbol (2-8 uppercase letters recommended).",
        name="Company display name.",
        location="Company location (lore).",
        industry="Company industry (lore).",
        founded_year="Founded year (lore, e.g. 1998).",
        description="Company lore description.",
        evaluation="Company evaluation/valuation lore.",
        base_price="Initial price. Recommended: 5 to 500.",
        slope="Base trend slope. Recommended: -5 to +5.",
        drift="Random drift percent per tick. Recommended: 0.5/price (min drift) to 3.0 (%).",
        liquidity="Order-flow depth (higher = less impact). Recommended: 20 to 500.",
        impact_power="Player impact exponent p. Recommended: 1.0 to 2.0 (1.0 = linear).",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def addcompany(
        interaction: Interaction,
        symbol: str,
        name: str,
        location: str,
        industry: str,
        founded_year: int,
        description: str,
        evaluation: str,
        base_price: float,
        slope: float,
        drift: float,
        liquidity: float,
        impact_power: float,
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
            location=location,
            industry=industry,
            founded_year=founded_year,
            description=description,
            evaluation=evaluation,
            base_price=base_price,
            slope=slope,
            drift=drift,
            liquidity=max(1.0, liquidity),
            impact_power=max(0.1, impact_power),
            starting_tick=current_tick,
            current_price=base_price,
            last_tick=current_tick,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

        if created:
            symbol_upper = symbol.upper()
            impact_curve = build_player_impact_curve(
                symbol=symbol_upper,
                slope=slope,
                drift=drift,
                liquidity=max(1.0, liquidity),
                impact_power=max(0.1, impact_power),
            )
            projection_curve = build_price_projection_curve(
                symbol=symbol_upper,
                start_price=base_price,
                slope=slope,
                drift=drift,
                liquidity=max(1.0, liquidity),
                impact_power=max(0.1, impact_power),
            )
            impact_embed = Embed(
                title=f"Company `{symbol_upper}` added",
                description="Player impact curve preview.",
            )
            impact_embed.set_image(url=f"attachment://{symbol_upper.lower()}_impact_curve.png")
            sim_embed = Embed(
                title=f"{symbol_upper} simulated path",
                description="No-trade simulation from current price/slope/drift.",
            )
            sim_embed.set_image(url=f"attachment://{symbol_upper.lower()}_projection_curve.png")
            await interaction.response.send_message(
                embeds=[impact_embed, sim_embed],
                files=[impact_curve, projection_curve],
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"Company `{symbol.upper()}` already exists.",
                ephemeral=True,
            )
