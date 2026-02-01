import asyncio
import time

import discord

from stockbot.commands import setup_commands
from stockbot.config.settings import TICK_INTERVAL, TOKEN
from stockbot.db import (
    backfill_company_starting_ticks,
    get_state_value,
    init_db,
    set_state_value,
)
from stockbot.services.economy import process_ticks


class StockBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)
        self._synced = False
        self._tick_task: asyncio.Task | None = None

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
        if self._tick_task is None:
            self._tick_task = asyncio.create_task(self._tick_loop())

    async def _tick_loop(self) -> None:
        if TICK_INTERVAL <= 0:
            return

        while not self.is_closed():
            last_tick_raw = get_state_value("last_tick")
            last_tick = int(last_tick_raw) if last_tick_raw is not None else 0
            next_tick = last_tick + 1

            tick_indices = [next_tick]
            guild_ids = [guild.id for guild in self.guilds]
            await asyncio.to_thread(process_ticks, tick_indices, guild_ids)
            set_state_value("last_tick", str(next_tick))
            set_state_value("last_tick_epoch", str(time.time()))

            await asyncio.sleep(TICK_INTERVAL)


def run() -> None:
    init_db()
    backfill_company_starting_ticks()
    bot = StockBot()
    bot.run(TOKEN)
