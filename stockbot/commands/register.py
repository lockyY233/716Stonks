from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import Embed, File, Interaction, app_commands
from discord.utils import get as get_role
from discord.ui import View, button

from stockbot.config import START_BALANCE, STONKERS_ROLE_NAME
from stockbot.core.ranks import Rank
from stockbot.db import register_user

REGISTER_REQUIRED_MESSAGE = "You are not registered yet. Use /register first."


async def _ensure_stonkers_role(guild: discord.Guild) -> discord.Role:
    role = get_role(guild.roles, name=STONKERS_ROLE_NAME)
    if role is None:
        role = await guild.create_role(name=STONKERS_ROLE_NAME, reason="Auto role for registered users")
    return role


async def _build_register_response(interaction: Interaction) -> tuple[Embed, File | None]:
    joined_at = datetime.now(timezone.utc).isoformat()
    joined_date = joined_at.split("T", maxsplit=1)[0]
    created = register_user(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        joined_at=joined_at,
        bank=START_BALANCE,
        display_name=interaction.user.display_name,
        rank=Rank.PRIVATE.value,
    )

    role_note = None
    if created:
        try:
            member = interaction.user
            if not isinstance(member, discord.Member):
                member = await interaction.guild.fetch_member(interaction.user.id)
            role = await _ensure_stonkers_role(interaction.guild)
            await member.add_roles(role, reason="User registered")
        except (discord.Forbidden, discord.HTTPException) as exc:
            role_note = f"Could not assign `{STONKERS_ROLE_NAME}` role: {exc.__class__.__name__}"

        embed = Embed(
            title=f"✅ Registered **SUCCESS** ✅",
            description=f"Hello {interaction.user.mention} ! \n Welcome to **Capitalism**! 📈",
        )
        embed.add_field(
            name="💳 **Balance**",
            value=f"${START_BALANCE}",
            inline=True,
        )
        embed.add_field(
            name="︽ **Rank**",
            value=Rank.PRIVATE.value.title(),
            inline=True,
        )
        embed.add_field(
            name="📅 **Joined**",
            value=joined_date,
            inline=True,
        )
        if role_note:
            embed.add_field(name="⚠️ Role", value=role_note, inline=False)
    else:
        embed = Embed(
            title="ALREADY REGISTERED",
            description="Your account already exists.",
        )

    avatar_url = interaction.user.display_avatar.url
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=avatar_url,
    )

    assets_dir = Path(__file__).resolve().parents[1] / "assets" / "images"
    banner_path = assets_dir / "Stonks.jpg"
    if banner_path.exists():
        banner = File(banner_path, filename="Stonks.jpg")
        embed.set_image(url="attachment://Stonks.jpg")
        return embed, banner
    return embed, None


class RegisterNowView(View):
    @button(label="Register", style=discord.ButtonStyle.green)
    async def register_now(self, interaction: Interaction, _button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        embed, banner = await _build_register_response(interaction)
        if banner is None:
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(embed=embed, file=banner, ephemeral=True)


def setup_register(tree: app_commands.CommandTree) -> None:
    @tree.command(name="register", description="Register your account.")
    async def register(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server."
            )
            return
        await interaction.response.defer(thinking=True)
        embed, banner = await _build_register_response(interaction)
        if banner is None:
            await interaction.followup.send(embed=embed)
            return
        await interaction.followup.send(embed=embed, file=banner)
