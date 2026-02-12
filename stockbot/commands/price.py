from datetime import datetime, timezone
from io import BytesIO
from zoneinfo import ZoneInfo

import matplotlib
from discord import ButtonStyle, Embed, File, Interaction, app_commands
from discord.errors import NotFound
from discord.ui import View, button

matplotlib.use("Agg")
from matplotlib import pyplot as plt

from stockbot.config import DISPLAY_TIMEZONE
from stockbot.commands.trade_views import BuyConfirmView, SellConfirmView
from stockbot.db import (
    get_companies,
    get_company,
    get_latest_close,
    get_price_history,
    get_user_shares,
)


class PriceView(View):
    def __init__(self, symbol: str) -> None:
        super().__init__()
        self.symbol = symbol

    @button(label="Buy", style=ButtonStyle.green)
    async def buy(self, interaction: Interaction, _button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        company = get_company(interaction.guild.id, self.symbol)
        if company is not None:
            price = float(company.get("current_price", company["base_price"]))
            message = f"Confirm buy action for {self.symbol} @ **${price:.2f}**:"
        else:
            message = "Confirm buy action:"
        await interaction.response.send_message(
            message,
            view=BuyConfirmView(self.symbol),
            ephemeral=True,
        )

    @button(label="Sell", style=ButtonStyle.red)
    async def sell(self, interaction: Interaction, _button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        company = get_company(interaction.guild.id, self.symbol)
        if company is not None:
            price = float(company.get("current_price", company["base_price"]))
            message = f"Confirm sell action for {self.symbol} @ **${price:.2f}**:"
        else:
            message = "Confirm sell action:"
        await interaction.response.send_message(
            message,
            view=SellConfirmView(self.symbol),
            ephemeral=True,
        )


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
        try:
            await interaction.response.defer(thinking=True)
        except NotFound:
            # Interaction token expired before we could acknowledge it.
            return

        default_limit = 80
        if last is None:
            rows = get_price_history(
                guild_id=interaction.guild.id,
                symbol=symbol.upper(),
                limit=default_limit,
            )
        else:
            limit = max(1, min(last, 200))
            rows = get_price_history(
                guild_id=interaction.guild.id,
                symbol=symbol.upper(),
                limit=limit,
            )
            # If requested history is larger than available, the query naturally
            # returns all rows back to the earliest available point.
        if not rows:
            try:
                await interaction.followup.send(
                    f"No price history for `{symbol.upper()}` yet.",
                    ephemeral=True,
                )
            except NotFound:
                return
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

        company_row = get_company(interaction.guild.id, symbol.upper())
        current_price = None
        base_price = None
        company_name = None
        founded_year = None
        if company_row is not None:
            current_price = company_row.get("current_price")
            base_price = company_row.get("base_price")
            company_name = company_row.get("name")
            founded_year = company_row.get("founded_year")

        embed = Embed(
            title=f"{symbol.upper()} Price",
            description=company_name or "",
        )
        change_value = None
        if current_price is not None:
            embed.add_field(
                name="Current Price",
                value=f"**${current_price:.2f}**",
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
            if founded_year:
                embed.add_field(
                    name="Founded",
                    value=str(founded_year),
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

        view = PriceView(symbol.upper())
        try:
            await interaction.followup.send(embed=embed, file=chart_file, view=view)
        except NotFound:
            return
