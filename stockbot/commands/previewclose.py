import asyncio

import discord
from discord import Interaction, app_commands

from stockbot.config.runtime import get_app_config
from stockbot.db import get_close_news, get_state_value, get_top_users_by_networth, recalc_all_networth
from stockbot.services.announcements import (
    build_close_news_embed,
    build_close_ranking_lines,
    build_market_close_v2_view,
)


def setup_previewclose(tree: app_commands.CommandTree) -> None:
    @tree.command(name="previewclose", description="Admin: preview market-close announcement privately.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def previewclose(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        await asyncio.to_thread(recalc_all_networth, guild.id)
        top = await asyncio.to_thread(get_top_users_by_networth, guild.id, 3)
        announcement_md = get_state_value(f"close_announcement_md:{guild.id}") or ""
        stonkers_role_name = str(get_app_config("STONKERS_ROLE_NAME"))
        role = discord.utils.get(guild.roles, name=stonkers_role_name)
        role_mention = role.mention if role is not None else f"@{stonkers_role_name}"

        lines = build_close_ranking_lines(top)

        v2_view = build_market_close_v2_view(
            announcement_md,
            lines,
            role_mention=f"{role_mention} *(preview)*",
            timeout=300,
        )
        if v2_view is None:
            await interaction.followup.send(
                "V2 preview unavailable in this runtime.",
                ephemeral=True,
            )
            return

        try:
            await interaction.followup.send(
                view=v2_view,
                ephemeral=True,
            )
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"V2 preview rejected by Discord API: {exc}",
                ephemeral=True,
            )
            return

        news_rows = await asyncio.to_thread(get_close_news, guild.id, enabled_only=True)
        if news_rows:
            for row in news_rows:
                news_embed = build_close_news_embed(row, preview=True)
                await interaction.followup.send(embed=news_embed, ephemeral=True)
        else:
            await interaction.followup.send("No enabled close-news embeds to preview.", ephemeral=True)
