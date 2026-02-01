from datetime import datetime, timezone
from io import BytesIO
from zoneinfo import ZoneInfo

import matplotlib
from discord import Embed, File, Interaction, app_commands

matplotlib.use("Agg")
from matplotlib import pyplot as plt

from stockbot.config import DISPLAY_TIMEZONE
from stockbot.db import get_companies, get_latest_close, get_price_history, get_user_shares


def setup_price(tree: app_commands.CommandTree) -> None:
    @tree.command(name="price", description="Show a price chart for a company.")
    async def price(
        interaction: Interaction,
        symbol: str,
        last: int | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        if last is None:
            rows = get_price_history(
                guild_id=interaction.guild.id,
                symbol=symbol.upper(),
                limit=None,
            )
        else:
            limit = max(1, min(last, 200))
            rows = get_price_history(
                guild_id=interaction.guild.id,
                symbol=symbol.upper(),
                limit=limit,
            )
        if not rows:
            await interaction.response.send_message(
                f"No price history for `{symbol.upper()}` yet.",
                ephemeral=True,
            )
            return

        max_points = 200
        min_marker_points = 30
        if len(rows) > max_points:
            step = max(1, len(rows) // max_points)
            sampled = rows[::step]
            if sampled[-1] != rows[-1]:
                sampled.append(rows[-1])
            rows = sampled

        try:
            tz = ZoneInfo(DISPLAY_TIMEZONE)
        except Exception:
            tz = timezone.utc

        labels = []
        for row in rows:
            dt = datetime.fromisoformat(row["ts"].replace("Z", "+00:00")).astimezone(tz)
            labels.append(dt.strftime("%m/%d %H:%M"))
        prices = [row["price"] for row in rows]

        fig, ax = plt.subplots(figsize=(6.5, 5))
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)
        x_values = list(range(len(labels)))
        if len(rows) <= min_marker_points:
            ax.plot(
                x_values,
                prices,
                color="#3ae280cc",
                linewidth=3.0,
                marker="o",
                markersize=10,
            )
        else:
            ax.plot(
                x_values,
                prices,
                color="#2ecc71",
                linewidth=3.0,
            )
        ax.set_title(f"{symbol.upper()} Price", fontsize=18, color="#666666")
        ax.set_xlabel("Time", fontsize=14, color="#666666")
        ax.set_ylabel("Price", fontsize=14, color="#666666")
        if len(labels) > 1:
            step = max(1, len(labels) // 8)
            ax.set_xticks(list(range(0, len(labels), step)))
            ax.set_xticklabels(labels[::step], rotation=45, ha="right", fontsize=12, color="#666666")
        else:
            ax.tick_params(axis="x", rotation=45, labelsize=12, colors="#666666")
        ax.tick_params(axis="y", labelsize=12, colors="#666666")
        if len(labels) > 1:
            ax.set_xlim(0, len(labels) - 1)
        ax.grid(True, alpha=0.2, color="#999999")

        buf = BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png", dpi=140, transparent=True)
        plt.close(fig)
        buf.seek(0)
        chart_file = File(buf, filename="price.png")

        current_price = None
        base_price = None
        created_at = None
        for company in get_companies(interaction.guild.id):
            if company["symbol"] == symbol.upper():
                current_price = company.get("current_price")
                base_price = company.get("base_price")
                created_at = company.get("updated_at")
                break

        company_name = None
        for company in get_companies(interaction.guild.id):
            if company["symbol"] == symbol.upper():
                company_name = company.get("name")
                break

        embed = Embed(
            title=f"{symbol.upper()} Price",
            description=company_name or "",
        )
        change_value = None
        if current_price is not None:
            embed.add_field(
                name="Current Price",
                value=f"**${current_price}**",
                inline=True,
            )
            latest_close = get_latest_close(interaction.guild.id, symbol.upper())
            if latest_close and latest_close.get("close_price"):
                prev_close = float(latest_close["close_price"])
                if prev_close != 0:
                    change = ((current_price - prev_close) / prev_close) * 100.0
                    change_value = change
                    arrow = "🟢" if change >= 0 else "🔴"
                    embed.add_field(
                        name="Daily Change",
                        value=f"**{arrow} {change:+.2f}%**",
                        inline=True,
                    )
            elif base_price:
                change = ((current_price - float(base_price)) / float(base_price)) * 100.0
                change_value = change
                arrow = "🟢" if change >= 0 else "🔴"
                embed.add_field(
                    name="Daily Change",
                    value=f"**{arrow} {change:+.2f}%**",
                    inline=True,
                )
                if created_at:
                    try:
                        tz = ZoneInfo(DISPLAY_TIMEZONE)
                    except Exception:
                        tz = timezone.utc
                    created_dt = datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    ).astimezone(tz)
                    embed.add_field(
                        name="Created",
                        value=created_dt.strftime("%m/%d"),
                        inline=True,
                    )
            owned = get_user_shares(
                guild_id=interaction.guild.id,
                user_id=interaction.user.id,
                symbol=symbol.upper(),
            )
            embed.add_field(
                name="You Own",
                value=str(owned),
                inline=True,
            )
        if change_value is not None:
            embed.color = 0x2ecc71 if change_value >= 0 else 0xe74c3c
        embed.set_image(url="attachment://price.png")

        await interaction.response.send_message(embed=embed, file=chart_file)
