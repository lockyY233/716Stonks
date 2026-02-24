from discord import ButtonStyle, Interaction
from discord.ui import Modal, TextInput, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config.runtime import get_app_config
from stockbot.db import get_commodity, get_company
from stockbot.services.trading import (
    perform_buy,
    perform_buy_commodity,
    perform_sell,
    preview_sell_transaction,
)


class BuySharesModal(Modal):
    def __init__(self, symbol: str) -> None:
        super().__init__(title=f"Buy {symbol}")
        self.symbol = symbol
        self.shares = TextInput(label="Shares", placeholder="Enter amount", required=True)
        self.add_item(self.shares)

    async def on_submit(self, interaction: Interaction) -> None:
        try:
            shares = int(self.shares.value)
        except ValueError:
            await interaction.response.send_message(
                "Shares must be a whole number.",
                ephemeral=True,
            )
            return
        _ok, message = await perform_buy(interaction, self.symbol, shares)
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)


class SellSharesModal(Modal):
    def __init__(self, symbol: str) -> None:
        super().__init__(title=f"Sell {symbol}")
        self.symbol = symbol
        self.shares = TextInput(label="Shares", placeholder="Enter amount", required=True)
        self.add_item(self.shares)

    async def on_submit(self, interaction: Interaction) -> None:
        try:
            shares = int(self.shares.value)
        except ValueError:
            await interaction.response.send_message(
                "Shares must be a whole number.",
                ephemeral=True,
            )
            return
        _ok, message = await perform_sell(interaction, self.symbol, shares)
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)


class BuyConfirmView(View):
    def __init__(self, symbol: str) -> None:
        super().__init__()
        self.symbol = symbol

    @button(label="Buy 1 Share", style=ButtonStyle.green)
    async def buy_one(self, interaction: Interaction, _button) -> None:
        _ok, message = await perform_buy(interaction, self.symbol, 1)
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)

    @button(label="Buy Shares", style=ButtonStyle.blurple)
    async def buy_many(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(BuySharesModal(self.symbol))


class SellConfirmView(View):
    def __init__(self, symbol: str) -> None:
        super().__init__()
        self.symbol = symbol

    @button(label="Sell 1 Share", style=ButtonStyle.red)
    async def sell_one(self, interaction: Interaction, _button) -> None:
        _ok, message = await perform_sell(interaction, self.symbol, 1)
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)

    @button(label="Sell Shares", style=ButtonStyle.blurple)
    async def sell_many(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(SellSharesModal(self.symbol))


class ChooseBuySymbolModal(Modal):
    def __init__(self) -> None:
        super().__init__(title="Buy - Choose Stock")
        self.symbol = TextInput(label="Symbol", placeholder="e.g. STONK", required=True)
        self.add_item(self.symbol)

    async def on_submit(self, interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        symbol = self.symbol.value.strip().upper()
        company = get_company(interaction.guild.id, symbol)
        if company is None:
            await interaction.response.send_message(
                f"Company `{symbol}` not found.",
                ephemeral=True,
            )
            return
        price = float(company.get("current_price", company["base_price"]))
        fee_pct = max(0.0, float(get_app_config("TRADING_FEES")))
        total = price
        await interaction.response.send_message(
            (
                f"Confirm buy action for {symbol} @ 💰**${price:.2f}**:\n"
                f"Transaction fee: {fee_pct:.2f}% on realized sell profit only (buy fee: $0.00).\n"
                f"Total (1 share): **${total:.2f}**"
            ),
            view=BuyConfirmView(symbol),
            ephemeral=True,
        )


class BuyCommodityQuantityModal(Modal):
    def __init__(self, commodity_name: str) -> None:
        super().__init__(title=f"Buy Commodity - {commodity_name}")
        self.commodity_name = commodity_name
        self.quantity = TextInput(label="Quantity", placeholder="Enter amount", required=True)
        self.add_item(self.quantity)

    async def on_submit(self, interaction: Interaction) -> None:
        try:
            quantity = int(self.quantity.value)
        except ValueError:
            await interaction.response.send_message(
                "Quantity must be a whole number.",
                ephemeral=True,
            )
            return
        _ok, message = await perform_buy_commodity(interaction, self.commodity_name, quantity)
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)


class ChooseBuyCommodityModal(Modal):
    def __init__(self) -> None:
        super().__init__(title="Buy - Choose Commodity")
        self.name = TextInput(label="Commodity name", placeholder="e.g. Gold", required=True)
        self.add_item(self.name)

    async def on_submit(self, interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        commodity_name = self.name.value.strip()
        commodity = get_commodity(interaction.guild.id, commodity_name)
        if commodity is None:
            await interaction.response.send_message(
                f"Commodity `{commodity_name}` not found.",
                ephemeral=True,
            )
            return
        price = float(commodity["price"])
        await interaction.response.send_message(
            f"Buy commodity {commodity['name']} @ 💰**${price:.2f}** each. Enter quantity:",
            view=BuyCommodityConfirmView(str(commodity["name"])),
            ephemeral=True,
        )


class BuyCommodityConfirmView(View):
    def __init__(self, commodity_name: str) -> None:
        super().__init__()
        self.commodity_name = commodity_name

    @button(label="Buy 1", style=ButtonStyle.green)
    async def buy_one(self, interaction: Interaction, _button) -> None:
        _ok, message = await perform_buy_commodity(interaction, self.commodity_name, 1)
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)

    @button(label="Buy Quantity", style=ButtonStyle.blurple)
    async def buy_many(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(BuyCommodityQuantityModal(self.commodity_name))


class ChooseSellSymbolModal(Modal):
    def __init__(self) -> None:
        super().__init__(title="Sell - Choose Stock")
        self.symbol = TextInput(label="Symbol", placeholder="e.g. STONK", required=True)
        self.add_item(self.symbol)

    async def on_submit(self, interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        symbol = self.symbol.value.strip().upper()
        company = get_company(interaction.guild.id, symbol)
        if company is None:
            await interaction.response.send_message(
                f"Company `{symbol}` not found.",
                ephemeral=True,
            )
            return
        price = float(company.get("current_price", company["base_price"]))
        preview = preview_sell_transaction(
            interaction.guild.id,
            interaction.user.id,
            symbol,
            shares=1,
        )
        if preview is None:
            fee_line = "Transaction fee: calculated at execution time."
            total_line = f"Total (1 share): **${price:.2f}** (gross)."
            avg_buy_line = "Avg buy price: unavailable."
            profit_line = "Estimated P/L: unavailable."
        else:
            fee_line = f"Transaction fee: {preview['fee_percent']:.2f}% => ${preview['fee']:.2f}."
            avg_buy_line = f"Avg buy price: ${preview['avg_buy_price']:.2f}/share."
            profit_line = (
                f"Estimated P/L: **${preview['estimated_pl_after_fee']:+.2f}** "
                f"(before fee ${preview['estimated_pl_before_fee']:+.2f})."
            )
            total_line = (
                f"Total (1 share): **${preview['net_gain']:.2f}** "
                f"(gross ${preview['gross_gain']:.2f})."
            )
        await interaction.response.send_message(
            (
                f"Confirm sell action for {symbol} @ 💰**${price:.2f}**:\n"
                f"{avg_buy_line}\n"
                f"{fee_line}\n"
                f"{profit_line}\n"
                f"{total_line}"
            ),
            view=SellConfirmView(symbol),
            ephemeral=True,
        )

# Buying stocks Button under buy
class BuyStockStart(View):
    @button(label="Stocks", style=ButtonStyle.green)
    async def choose_stock(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(ChooseBuySymbolModal())

    @button(label="Commodities", style=ButtonStyle.blurple)
    async def choose_commodity(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(ChooseBuyCommodityModal())

# Selling stocks Button under buy
class SellStockStart(View):
    @button(label="Stocks", style=ButtonStyle.red)
    async def choose_stock(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(ChooseSellSymbolModal())
