from discord import Interaction, app_commands

from stockbot.db import purge_all_data


CONFIRM_TEXT = "IUNDERSTANDTHISACTIONWILLDELETEALLDATA"


def setup_purgeall(tree: app_commands.CommandTree) -> None:
    @tree.command(name="purgeall", description="ADMIN: WIPE ALL DATA AND RESET THE BOT.")
    @app_commands.checks.has_permissions(administrator=True)
    async def purgeall(
        interaction: Interaction,
        confirm: str,
    ) -> None:
        if confirm != CONFIRM_TEXT:
            await interaction.response.send_message(
                "Confirmation text mismatch. No data was deleted.",
                ephemeral=True,
            )
            return

        purge_all_data()
        await interaction.response.send_message(
            "All data deleted and bot reset.",
            ephemeral=True,
        )
