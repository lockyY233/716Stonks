from datetime import datetime, timezone
from io import BytesIO
import math
from zoneinfo import ZoneInfo

import matplotlib
from discord import ButtonStyle, Embed, File, Interaction, SelectOption, app_commands
from discord.errors import NotFound
from discord.ui import Button, Select, View, button

matplotlib.use("Agg")
from matplotlib import pyplot as plt

from stockbot.config.runtime import get_app_config
from stockbot.commands.trade_views import BuyConfirmView, SellConfirmView
from stockbot.db import (
    get_company,
    get_companies,
    get_latest_close,
    get_price_history,
    get_user_shares,
)
from stockbot.services.trading import preview_sell_transaction


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
            preview = preview_sell_transaction(
                interaction.guild.id,
                interaction.user.id,
                self.symbol,
                shares=1,
            )
            if preview is None:
                message = (
                    f"Confirm sell action for {self.symbol} @ **${price:.2f}**:\n"
                    f"Avg buy price: unavailable.\n"
                    f"Transaction fee: calculated at execution time.\n"
                    f"Estimated P/L: unavailable.\n"
                    f"Total (1 share): **${price:.2f}** (gross)."
                )
            else:
                message = (
                    f"Confirm sell action for {self.symbol} @ **${price:.2f}**:\n"
                    f"Avg buy price: ${preview['avg_buy_price']:.2f}/share.\n"
                    f"Transaction fee: {preview['fee_percent']:.2f}% => ${preview['fee']:.2f}.\n"
                    f"Estimated P/L: **${preview['estimated_pl_after_fee']:+.2f}** "
                    f"(before fee ${preview['estimated_pl_before_fee']:+.2f}).\n"
                    f"Total (1 share): **${preview['net_gain']:.2f}** "
                    f"(gross ${preview['gross_gain']:.2f})."
                )
        else:
            message = "Confirm sell action:"
        await interaction.response.send_message(
            message,
            view=SellConfirmView(self.symbol),
            ephemeral=True,
        )

    @button(label="Desc", style=ButtonStyle.secondary)
    async def desc(self, interaction: Interaction, _button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        row = get_company(interaction.guild.id, self.symbol)
        if row is None:
            await interaction.response.send_message(
                f"Company `{self.symbol}` not found.",
                ephemeral=True,
            )
            return

        name = str(row.get("name", self.symbol))
        symbol = str(row.get("symbol", self.symbol)).upper()
        industry = str(row.get("industry", "") or "Unknown")
        founded_year = str(row.get("founded_year", "") or "Unknown")
        location = str(row.get("location", "") or "Unknown")
        evaluation = str(row.get("evaluation", "") or "Unknown")
        description = str(row.get("description", "") or "No lore description.")

        embed = Embed(
            title=f"{name} ({symbol})",
            description=description,
        )
        embed.add_field(name="Industry", value=industry, inline=True)
        embed.add_field(name="Founded", value=founded_year, inline=True)
        embed.add_field(name="Location", value=location, inline=True)
        embed.add_field(name="Evaluation", value=evaluation, inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class PriceSymbolPickerView(View):
    def __init__(self, owner_id: int, guild_id: int, symbols: list[tuple[str, str]], last: int | None) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._guild_id = guild_id
        self._symbols = symbols
        self._last = last
        self._page = 0
        self._pages = max(1, math.ceil(len(self._symbols) / 25))
        self._render()

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can interact with this selector.",
                ephemeral=True,
            )
            return False
        return True

    def _render(self) -> None:
        self.clear_items()
        start = self._page * 25
        end = min(len(self._symbols), start + 25)
        options = [
            SelectOption(label=f"{symbol} · {name[:70]}", value=symbol)
            for symbol, name in self._symbols[start:end]
        ]
        picker = Select(
            placeholder=f"Choose a company ({self._page + 1}/{self._pages})",
            min_values=1,
            max_values=1,
            options=options,
        )

        async def on_pick(interaction: Interaction) -> None:
            symbol = str(picker.values[0]).upper().strip()
            try:
                await interaction.response.defer(thinking=True)
            except NotFound:
                return
            embed, chart_file, view, error = build_price_payload(
                guild_id=self._guild_id,
                user_id=interaction.user.id,
                symbol=symbol,
                last=self._last,
            )
            if error is not None:
                try:
                    await interaction.followup.send(error, ephemeral=True)
                except NotFound:
                    return
                return
            try:
                await interaction.followup.send(embed=embed, file=chart_file, view=view)
            except NotFound:
                return

        picker.callback = on_pick
        self.add_item(picker)

        if self._pages > 1:
            prev_btn = Button(
                label="Prev",
                style=ButtonStyle.secondary,
                disabled=self._page <= 0,
            )
            next_btn = Button(
                label="Next",
                style=ButtonStyle.secondary,
                disabled=self._page >= self._pages - 1,
            )

            async def on_prev(interaction: Interaction) -> None:
                self._page = max(0, self._page - 1)
                self._render()
                await interaction.response.edit_message(view=self)

            async def on_next(interaction: Interaction) -> None:
                self._page = min(self._pages - 1, self._page + 1)
                self._render()
                await interaction.response.edit_message(view=self)

            prev_btn.callback = on_prev
            next_btn.callback = on_next
            self.add_item(prev_btn)
            self.add_item(next_btn)


def build_price_payload(
    guild_id: int,
    user_id: int,
    symbol: str,
    last: int | None = None,
) -> tuple[Embed | None, File | None, PriceView | None, str | None]:
    symbol = symbol.upper()
    default_limit = 80
    if last is None:
        rows = get_price_history(
            guild_id=guild_id,
            symbol=symbol,
            limit=default_limit,
        )
    else:
        limit = max(1, min(last, 200))
        rows = get_price_history(
            guild_id=guild_id,
            symbol=symbol,
            limit=limit,
        )
    if not rows:
        return None, None, None, f"No price history for `{symbol}` yet."

    max_points = 200
    min_marker_points = 30
    if len(rows) > max_points:
        step = max(1, len(rows) // max_points)
        sampled = rows[::step]
        if sampled[-1] != rows[-1]:
            sampled.append(rows[-1])
        rows = sampled

    display_timezone = str(get_app_config("DISPLAY_TIMEZONE"))
    try:
        tz = ZoneInfo(display_timezone)
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
    ax.set_title(f"{symbol} Price", fontsize=18, color="#666666")
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

    company_row = get_company(guild_id, symbol)
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
        title=f"{symbol} Price",
        description=company_name or "",
    )
    change_value = None
    if current_price is not None:
        embed.add_field(
            name="Current Price",
            value=f"**${current_price:.2f}**",
            inline=True,
        )
        latest_close = get_latest_close(guild_id, symbol)
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
            guild_id=guild_id,
            user_id=user_id,
            symbol=symbol,
        )
        embed.add_field(
            name="You Own",
            value=str(owned),
            inline=True,
        )
    if change_value is not None:
        embed.color = 0x2ecc71 if change_value >= 0 else 0xe74c3c
    embed.set_image(url="attachment://price.png")
    return embed, chart_file, PriceView(symbol), None


def setup_price(tree: app_commands.CommandTree) -> None:
    @tree.command(name="price", description="Show a price chart for a company.")
    @app_commands.describe(
        symbol="Company symbol. Leave blank to pick from dropdown.",
        last="How many latest price points to include.",
    )
    async def price(
        interaction: Interaction,
        symbol: str | None = None,
        last: int | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        if symbol is None or not str(symbol).strip():
            rows = get_companies(interaction.guild.id)
            if not rows:
                await interaction.response.send_message(
                    "No companies found.",
                    ephemeral=True,
                )
                return
            options = [
                (str(row.get("symbol", "")).upper(), str(row.get("name", "Unknown")))
                for row in rows
                if str(row.get("symbol", "")).strip()
            ]
            if not options:
                await interaction.response.send_message(
                    "No companies found.",
                    ephemeral=True,
                )
                return
            options.sort(key=lambda x: x[0])
            await interaction.response.send_message(
                "Select a company to show price chart:",
                view=PriceSymbolPickerView(
                    owner_id=interaction.user.id,
                    guild_id=interaction.guild.id,
                    symbols=options,
                    last=last,
                ),
                ephemeral=True,
            )
            return

        try:
            await interaction.response.defer(thinking=True)
        except NotFound:
            # Interaction token expired before we could acknowledge it.
            return

        embed, chart_file, view, error = build_price_payload(
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            symbol=symbol,
            last=last,
        )
        if error is not None:
            try:
                await interaction.followup.send(
                    error,
                    ephemeral=True,
                )
            except NotFound:
                return
            return
        try:
            await interaction.followup.send(embed=embed, file=chart_file, view=view)
        except NotFound:
            return
