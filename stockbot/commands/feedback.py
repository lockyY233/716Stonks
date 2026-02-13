from datetime import datetime, timezone

import discord
from discord import Interaction, app_commands
from discord.ui import Modal, TextInput

from stockbot.db import add_feedback


class FeedbackModal(Modal):
    def __init__(self, guild_id: int) -> None:
        super().__init__(title="Send Anonymous Feedback")
        self._guild_id = guild_id
        self.message = TextInput(
            label="Feedback",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=1500,
            placeholder="Share your feedback. This is anonymous.",
        )
        self.add_item(self.message)

    async def on_submit(self, interaction: Interaction) -> None:
        text = self.message.value.strip()
        if not text:
            await interaction.response.send_message(
                "Feedback cannot be empty.",
                ephemeral=True,
            )
            return

        add_feedback(
            guild_id=self._guild_id,
            message=text,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        await interaction.response.send_message(
            "Feedback submitted anonymously. Thank you.",
            ephemeral=True,
        )


def setup_feedback(tree: app_commands.CommandTree) -> None:
    @tree.command(name="feedback", description="Send anonymous feedback.")
    async def feedback(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(FeedbackModal(interaction.guild.id))
