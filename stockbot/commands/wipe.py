from discord import Interaction, app_commands

from stockbot.db import wipe_companies, wipe_price_history, wipe_users


CONFIRM_TEXT = "WIPE"


def setup_wipe(tree: app_commands.CommandTree) -> None:
    @tree.command(name="wipe", description="Admin: wipe selected guild data.")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        target="What data to wipe.",
        confirm="Type WIPE to confirm.",
    )
    @app_commands.choices(
        target=[
            app_commands.Choice(name="price_history", value="price_history"),
            app_commands.Choice(name="users", value="users"),
            app_commands.Choice(name="companies", value="companies"),
        ]
    )
    async def wipe(
        interaction: Interaction,
        target: str,
        confirm: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        if confirm != CONFIRM_TEXT:
            await interaction.response.send_message(
                "Confirmation mismatch. Type `WIPE` to proceed.",
                ephemeral=True,
            )
            return

        guild_id = interaction.guild.id
        if target == "price_history":
            wipe_price_history(guild_id)
        elif target == "users":
            wipe_users(guild_id)
        elif target == "companies":
            wipe_companies(guild_id)
        else:
            await interaction.response.send_message(
                f"Unknown wipe target: `{target}`.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Wiped `{target}` for this server.",
            ephemeral=True,
        )
