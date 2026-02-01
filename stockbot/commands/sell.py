from discord import Interaction, app_commands

from stockbot.commands.trade_views import SellStockStart


def setup_sell(tree: app_commands.CommandTree) -> None:
    @tree.command(name="sell", description="Sell shares of a company.")
    async def sell(interaction: Interaction) -> None:
        await interaction.response.send_message(
            "What do you intend to sell?",
            view=SellStockStart(),
            ephemeral=True,
        )
