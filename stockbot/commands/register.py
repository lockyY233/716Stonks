from datetime import datetime, timezone
from pathlib import Path

from discord import Embed, File, Interaction, app_commands

from stockbot.config import START_BALANCE
from stockbot.core.ranks import Rank
from stockbot.db import register_user


def setup_register(tree: app_commands.CommandTree) -> None:
    @tree.command(name="register", description="Register your account.")
    async def register(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server."
            )
            return

        joined_at = datetime.now(timezone.utc).isoformat()
        joined_date = joined_at.split("T", maxsplit=1)[0]
        created = register_user(
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            joined_at=joined_at,
            bank=START_BALANCE,
            rank=Rank.PRIVATE.value,
        )

        if created:
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
            await interaction.response.send_message(embed=embed, file=banner)
        else:
            await interaction.response.send_message(embed=embed)
