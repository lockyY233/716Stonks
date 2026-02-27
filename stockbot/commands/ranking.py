from discord import Embed, Interaction, app_commands

from stockbot.config.runtime import get_app_config
from stockbot.db import recalc_all_networth
from stockbot.services.ranking import MIN_RANKING_NOTE, get_ranked_users_with_effective_networth


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
        rows = get_ranked_users_with_effective_networth(
            interaction.guild.id,
            limit=500,
            exclude_user_id=gm_id,
        )

        top_rows = rows[:5]
        lines: list[str] = []
        for idx, row in enumerate(top_rows, start=1):
            user_id = int(row["user_id"])
            name = str(row.get("display_name", "")).strip() or f"User {user_id}"
            final_networth = float(row.get("networth", 0.0))
            top_bonus = float(row.get("top_networth_bonus", 0.0))
            display_base = float(row.get("display_base_networth", final_networth - top_bonus))
            if idx == 1:
                prefix = "🥇"
            elif idx == 2:
                prefix = "🥈"
            elif idx == 3:
                prefix = "🥉"
            else:
                prefix = f"#{idx}"
            if abs(top_bonus) > 1e-9:
                sign = "+" if top_bonus >= 0 else "-"
                lines.append(f"**{prefix}** {name} — `${display_base:.2f}` {sign} **`${abs(top_bonus):.2f}`**")
            else:
                lines.append(f"**{prefix}** {name} — `${final_networth:.2f}`")

        sender_id = interaction.user.id
        sender_rank = next(
            (idx for idx, row in enumerate(rows, start=1) if int(row["user_id"]) == sender_id),
            None,
        )
        if sender_rank is not None and sender_rank > 5:
            sender_row = rows[sender_rank - 1]
            sender_name = str(sender_row.get("display_name", "")).strip() or f"User {sender_id}"
            sender_final = float(sender_row.get("networth", 0.0))
            sender_top_bonus = float(sender_row.get("top_networth_bonus", 0.0))
            sender_display_base = float(sender_row.get("display_base_networth", sender_final - sender_top_bonus))
            lines.append("")
            if abs(sender_top_bonus) > 1e-9:
                sign = "+" if sender_top_bonus >= 0 else "-"
                lines.append(
                    f"**Your Rank:** #{sender_rank} {sender_name} — `${sender_display_base:.2f}` {sign} **`${abs(sender_top_bonus):.2f}`**"
                )
            else:
                lines.append(f"**Your Rank:** #{sender_rank} {sender_name} — `${sender_final:.2f}`")

        if len(rows) < 5:
            if lines:
                lines.append("")
            lines.append(MIN_RANKING_NOTE)
        if not lines:
            lines.append(MIN_RANKING_NOTE)

        embed = Embed(
            title="Top 5 Networth Ranking",
            description="\n".join(lines),
        )
        await interaction.response.send_message(embed=embed)
