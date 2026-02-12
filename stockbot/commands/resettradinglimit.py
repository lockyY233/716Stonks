from discord import Interaction, app_commands

from stockbot.db import clear_state_prefix


def setup_resettradinglimit(tree: app_commands.CommandTree) -> None:
    @tree.command(name="resettradinglimit", description="Admin: reset trading-limit usage for this server.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def resettradinglimit(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        deleted = clear_state_prefix(f"trade_used:{interaction.guild.id}:")
        await interaction.response.send_message(
            f"Trading limits reset. Cleared {deleted} usage record(s).",
            ephemeral=True,
        )
