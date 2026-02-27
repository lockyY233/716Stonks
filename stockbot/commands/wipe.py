from discord import Interaction, Member, app_commands

from stockbot.db import wipe_price_history, wipe_user_history, wipe_users


CONFIRM_TEXT = "WIPE"


def setup_wipe(tree: app_commands.CommandTree) -> None:
    @tree.command(name="wipe", description="Admin: wipe selected guild data.")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        target="What data to wipe.",
        target_user="Optional: only wipe this user when target is users or user_history.",
        confirm="Type WIPE to confirm.",
    )
    @app_commands.choices(
        target=[
            app_commands.Choice(name="price_history", value="price_history"),
            app_commands.Choice(name="users", value="users"),
            app_commands.Choice(name="user_history", value="user_history"),
        ]
    )
    async def wipe(
        interaction: Interaction,
        target: str,
        target_user: Member | None,
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
            if target_user is not None:
                await interaction.response.send_message(
                    "`target_user` can only be used when target is `users` or `user_history`.",
                    ephemeral=True,
                )
                return
            wipe_price_history(guild_id)
        elif target == "users":
            wipe_users(guild_id, target_user.id if target_user is not None else None)
        elif target == "user_history":
            wipe_user_history(guild_id, target_user.id if target_user is not None else None)
        else:
            await interaction.response.send_message(
                f"Unknown wipe target: `{target}`.",
                ephemeral=True,
            )
            return

        suffix = (
            (
                f" for user {target_user.mention}"
                if target in {"users", "user_history"} and target_user is not None
                else " for this server"
            )
        )
        await interaction.response.send_message(
            f"Wiped `{target}`{suffix}.",
            ephemeral=True,
        )
