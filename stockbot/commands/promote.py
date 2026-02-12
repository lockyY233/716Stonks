import discord
from discord import Interaction, Member, app_commands

from stockbot.config import RANK_INCOME
from stockbot.db import get_user, update_user_admin_fields


def _rank_lookup() -> dict[str, str]:
    return {key.lower(): key for key in RANK_INCOME.keys()}


def setup_promote(tree: app_commands.CommandTree) -> None:
    @tree.command(name="promote", description="Admin: set a player's rank.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        target="Player to promote.",
        rank="Target rank (must match configured rank list).",
    )
    async def promote(
        interaction: Interaction,
        target: Member,
        rank: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        user = get_user(interaction.guild.id, target.id)
        if user is None:
            await interaction.response.send_message(
                f"{target.mention} is not registered.",
                ephemeral=True,
            )
            return

        lookup = _rank_lookup()
        canonical = lookup.get(rank.strip().lower())
        if canonical is None:
            await interaction.response.send_message(
                "Invalid rank. Use one of the configured rank names exactly.",
                ephemeral=True,
            )
            return

        rank_role_names = set(RANK_INCOME.keys())
        rank_roles_on_member = [role for role in target.roles if role.name in rank_role_names]
        target_role = discord.utils.get(interaction.guild.roles, name=canonical)
        if target_role is None:
            await interaction.response.send_message(
                f"Role `{canonical}` does not exist in this server.",
                ephemeral=True,
            )
            return

        try:
            if rank_roles_on_member:
                await target.remove_roles(*rank_roles_on_member, reason=f"Promoted by {interaction.user}")
            await target.add_roles(target_role, reason=f"Promoted by {interaction.user}")
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to manage one or more of those roles.",
                ephemeral=True,
            )
            return
        except discord.HTTPException:
            await interaction.response.send_message(
                "Failed to update Discord roles for that member.",
                ephemeral=True,
            )
            return

        ok = update_user_admin_fields(
            guild_id=interaction.guild.id,
            user_id=target.id,
            updates={"rank": canonical},
        )
        if not ok:
            await interaction.response.send_message(
                "Failed to update rank.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"{target.mention} is now **{canonical}**.",
        )

    @promote.autocomplete("rank")
    async def promote_rank_autocomplete(
        _interaction: Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        query = current.strip().lower()
        names = list(RANK_INCOME.keys())
        if query:
            names = [name for name in names if query in name.lower()]
        return [app_commands.Choice(name=name, value=name) for name in names[:25]]
