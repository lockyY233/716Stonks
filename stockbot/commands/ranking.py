from discord import Embed, Interaction, app_commands

from stockbot.config.runtime import get_app_config
from stockbot.db import get_users, recalc_all_networth


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
        gm_id = int(get_app_config("GM_ID"))
        rows = get_users(interaction.guild.id)
        if gm_id > 0:
            rows = [row for row in rows if int(row.get("user_id", 0)) != gm_id]
        if not rows:
            await interaction.response.send_message("No players found.")
            return

        top_rows = rows[:5]
        lines: list[str] = []
        for idx, row in enumerate(top_rows, start=1):
            user_id = int(row["user_id"])
            networth = float(row.get("networth", 0.0))
            lines.append(f"**#{idx}** <@{user_id}> — `${networth:.2f}`")

        sender_id = interaction.user.id
        sender_rank = next(
            (idx for idx, row in enumerate(rows, start=1) if int(row["user_id"]) == sender_id),
            None,
        )
        if sender_rank is not None and sender_rank > 5:
            sender_row = rows[sender_rank - 1]
            sender_networth = float(sender_row.get("networth", 0.0))
            lines.append("")
            lines.append(f"**Your Rank:** #{sender_rank} <@{sender_id}> — `${sender_networth:.2f}`")

        embed = Embed(
            title="Top 5 Networth Ranking",
            description="\n".join(lines),
        )
        await interaction.response.send_message(embed=embed)
