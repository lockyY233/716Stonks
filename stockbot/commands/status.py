from pathlib import Path

from discord import Embed, File, Interaction, app_commands

from stockbot.db import get_user_status


def setup_status(tree: app_commands.CommandTree) -> None:
    @tree.command(name="status", description="Show your status and holdings.")
    async def status(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=False,
            )
            return

        data = get_user_status(
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
        )
        if data is None:
            await interaction.response.send_message(
                "You are not registered yet. Use /register first.",
                ephemeral=False,
            )
            return

        user = data["user"]
        holdings = data["holdings"]

        embed = Embed(
            title=f"📊 Status of {interaction.user.display_name}",
            description=f"{interaction.user.mention}"
        )
        embed.add_field(name="💳 Balance", value=f"**${user['bank']}**", inline=True)
        embed.add_field(name="︽ Rank", value=user["rank"].title(), inline=True)
        embed.add_field(name="📅 Joined", value=user["joined_at"][:10], inline=True)

        if holdings:
            lines = [f"{row['symbol']}: {row['shares']}" for row in holdings]
            embed.add_field(name="📈 Holdings", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="📈 Holdings", value="None", inline=False)

        avatar_url = interaction.user.display_avatar.url
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=avatar_url,
        )

        assets_dir = Path(__file__).resolve().parents[1] / "assets" / "images"
        banner_path = assets_dir / "Status.jpg"
        if banner_path.exists():
            banner = File(banner_path, filename="Status.jpg")
            embed.set_image(url="attachment://Status.jpg")
            await interaction.response.send_message(
                embed=embed,
                file=banner,
            )
        else:
            await interaction.response.send_message(embed=embed)
