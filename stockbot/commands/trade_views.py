from discord import ButtonStyle, Interaction
from discord.ui import Modal, TextInput, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_company
from stockbot.services.trading import perform_buy, perform_sell


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
        await interaction.response.send_message(
            f"Confirm buy action for {symbol} @ 💰**${price:.2f}**:",
            view=BuyConfirmView(symbol),
            ephemeral=True,
        )


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
        await interaction.response.send_message(
            f"Confirm sell action for {symbol} @ 💰**${price:.2f}**:",
            view=SellConfirmView(symbol),
            ephemeral=True,
        )

# Buying stocks Button under buy
class BuyStockStart(View):
    @button(label="Stocks", style=ButtonStyle.green)
    async def choose_stock(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(ChooseBuySymbolModal())

# Selling stocks Button under buy
class SellStockStart(View):
    @button(label="Stocks", style=ButtonStyle.red)
    async def choose_stock(self, interaction: Interaction, _button) -> None:
        await interaction.response.send_modal(ChooseSellSymbolModal())
