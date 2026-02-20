from __future__ import annotations

import discord
from discord import Interaction, app_commands

from stockbot.config.runtime import get_app_config


def setup_activity(tree: app_commands.CommandTree) -> None:
    @tree.command(
        name="activity",
        description="Open the configured Discord Activity launch page.",
    )
    async def activity(interaction: Interaction) -> None:
        app_id = int(get_app_config("ACTIVITY_APPLICATION_ID"))
        if app_id <= 0:
            await interaction.response.send_message(
                "Activity app ID is not configured. Set `ACTIVITY_APPLICATION_ID` first.",
                ephemeral=True,
            )
            return

        directory_url = f"https://discord.com/application-directory/{app_id}"
        view = discord.ui.View(timeout=None)
        view.add_item(discord.ui.Button(label="Open Activity", style=discord.ButtonStyle.link, url=directory_url))

        await interaction.response.send_message(
            (
                "Use Discord's app launcher flow to start this Activity.\n"
                "If the page opens in browser, open it in Discord client and click **Launch**."
            ),
            view=view,
            ephemeral=False,
        )
