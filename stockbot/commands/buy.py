from discord import Interaction, app_commands

from stockbot.commands.trade_views import BuyStockStart


def setup_buy(tree: app_commands.CommandTree) -> None:
    @tree.command(name="buy", description="Buy shares of a company.")
    async def buy(interaction: Interaction) -> None:
        await interaction.response.send_message(
            "What do you intend to buy?",
            view=BuyStockStart(),
            ephemeral=True,
        )
