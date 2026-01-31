import discord

from stockbot.config import TOKEN


class StockBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        await self.tree.sync()


bot = StockBot()


@bot.tree.command(name="hello", description="Say hello!")
async def hello(interaction: discord.Interaction) -> None:
    await interaction.response.send_message("Hello world!")


bot.run(TOKEN)
