from pathlib import Path

from discord import Embed, File, Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_user_status


def setup_status(tree: app_commands.CommandTree) -> None:
    @tree.command(name="status", description="Show your status and holdings.")
    async def status(
        interaction: Interaction,
        member: Member | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=False,
            )
            return

        target = member or interaction.user
        data = get_user_status(
            guild_id=interaction.guild.id,
            user_id=target.id,
        )
        if data is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                ephemeral=True,
                view=RegisterNowView(),
            )
            return

        user = data["user"]
        holdings = data["holdings"]

        embed = Embed(
            title=f"📊 Status of {target.display_name}",
            description=f"{target.mention}"
        )
        embed.add_field(name="💳 Balance", value=f"**${user['bank']}**", inline=True)
        embed.add_field(name="︽ Rank", value=user["rank"].title(), inline=True)
        embed.add_field(name="📅 Joined", value=user["joined_at"][:10], inline=True)

        if holdings:
            lines = [f"{row['symbol']}: {row['shares']}" for row in holdings]
            embed.add_field(name="📈 Holdings", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="📈 Holdings", value="None", inline=False)

        avatar_url = target.display_avatar.url
        embed.set_author(
            name=target.display_name,
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
