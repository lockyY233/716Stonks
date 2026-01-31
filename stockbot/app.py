import discord

from stockbot.commands import setup_commands
from stockbot.config.settings import TOKEN
from stockbot.db import init_db


class StockBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)
        self._synced = False

    async def setup_hook(self) -> None:
        setup_commands(self.tree)

    async def on_ready(self) -> None:
        if self._synced:
            return

        # Sync commands to every guild the bot is currently in.
        # this is to faster the command sync
        for guild in self.guilds:
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)

        self._synced = True


def run() -> None:
    init_db()
    bot = StockBot()
    bot.run(TOKEN)
