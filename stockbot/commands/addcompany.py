from datetime import datetime, timezone
from io import BytesIO

import matplotlib
from discord import Embed, File, Interaction, app_commands
from matplotlib import pyplot as plt

from stockbot.db import add_company, get_state_value

matplotlib.use("Agg")


def _build_player_impact_curve(
    *,
    symbol: str,
    slope: float,
    drift: float,
    liquidity: float,
) -> File:
    # Base delta per tick under simplified model:
    # delta_base = slope + (buy - sell)/liquidity
    span = 100
    x_values = list(range(-span, span + 1))
    y_values = [slope + (x / liquidity) for x in x_values]

    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    ax.axhline(0, color="#666666", linewidth=1.0, alpha=0.7)
    ax.axvline(0, color="#666666", linewidth=1.0, alpha=0.7)
    ax.plot(x_values, y_values, color="#2ecc71", linewidth=2.5)
    ax.set_title(f"{symbol} Base Delta Per Tick")
    ax.set_xlabel("Net Shares In Tick (buy - sell)")
    ax.set_ylabel("Delta Base Price")
    ax.grid(True, alpha=0.2)
    fig.text(
        0.02,
        0.01,
        f"slope={slope:.4f}  drift=±{drift:.4f}  liquidity={liquidity:.2f}",
        transform=fig.transFigure,
        va="bottom",
        ha="left",
        fontsize=9,
        color="#444444",
        bbox={"boxstyle": "round,pad=0.25", "facecolor": "#ffffffcc", "edgecolor": "#cccccc"},
    )

    buf = BytesIO()
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    buf.seek(0)
    return File(buf, filename=f"{symbol.lower()}_impact_curve.png")


def setup_addcompany(tree: app_commands.CommandTree) -> None:
    @tree.command(name="addcompany", description="Admin: add a company.")
    @app_commands.describe(
        symbol="Ticker symbol (2-8 uppercase letters recommended).",
        name="Company display name.",
        base_price="Initial price. Recommended: 5 to 500.",
        slope="Base trend slope. Recommended: -5 to +5.",
        drift="Random drift amount per tick. Recommended: 0.05 to 3.0.",
        liquidity="Order-flow depth (higher = less impact). Recommended: 20 to 500.",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def addcompany(
        interaction: Interaction,
        symbol: str,
        name: str,
        base_price: float,
        slope: float,
        drift: float,
        liquidity: float,
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
            liquidity=max(1.0, liquidity),
            starting_tick=current_tick,
            current_price=base_price,
            last_tick=current_tick,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

        if created:
            symbol_upper = symbol.upper()
            curve = _build_player_impact_curve(
                symbol=symbol_upper,
                slope=slope,
                drift=drift,
                liquidity=max(1.0, liquidity),
            )
            embed = Embed(
                title=f"Company `{symbol_upper}` added",
                description="Base update preview for this company.",
            )
            embed.set_image(url=f"attachment://{symbol_upper.lower()}_impact_curve.png")
            await interaction.response.send_message(
                embed=embed,
                file=curve,
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"Company `{symbol.upper()}` already exists.",
                ephemeral=True,
            )
