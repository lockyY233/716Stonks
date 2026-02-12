from discord import Embed, Interaction, app_commands

from stockbot.db import get_top_users_by_networth, recalc_all_networth


def setup_ranking(tree: app_commands.CommandTree) -> None:
    @tree.command(name="ranking", description="Show top 5 players by networth.")
    async def ranking(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        recalc_all_networth(interaction.guild.id)
        rows = get_top_users_by_networth(interaction.guild.id, limit=5)
        if not rows:
            await interaction.response.send_message("No players found.")
            return

        lines: list[str] = []
        for idx, row in enumerate(rows, start=1):
            user_id = int(row["user_id"])
            networth = float(row.get("networth", 0.0))
            lines.append(f"**#{idx}** <@{user_id}> — `${networth:.2f}`")

        embed = Embed(
            title="Top 5 Networth Ranking",
            description="\n".join(lines),
        )
        await interaction.response.send_message(embed=embed)
