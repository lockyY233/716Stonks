from discord import Embed, Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config.runtime import get_app_config
from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME
from stockbot.db import get_user
from stockbot.services.perks import evaluate_user_perks


def setup_perks(tree: app_commands.CommandTree) -> None:
    @tree.command(name="perks", description="Show active perk effects for a player.")
    async def perks(interaction: Interaction, member: Member | None = None) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=False,
            )
            return

        target = member or interaction.user
        user = get_user(interaction.guild.id, target.id)
        if user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return

        rank = str(user.get("rank", DEFAULT_RANK))
        lookup = {k.lower(): float(v) for k, v in RANK_INCOME.items()}
        base_income = lookup.get(rank.lower(), float(RANK_INCOME.get(DEFAULT_RANK, 0.0)))
        base_trade_limits = int(get_app_config("TRADING_LIMITS"))
        result = evaluate_user_perks(
            guild_id=interaction.guild.id,
            user_id=target.id,
            base_income=base_income,
            base_trade_limits=base_trade_limits,
            base_networth=None,
        )
        matched = list(result.get("matched_perks", []))
        base_stats = result["base"]
        final_stats = result["final"]

        embed = Embed(
            title=f"Perks for {target.display_name}",
            description=target.mention,
        )
        embed.add_field(name="Rank", value=rank, inline=True)
        embed.add_field(
            name="Income",
            value=f"${float(base_stats['income']):.2f} -> **${float(final_stats['income']):.2f}**",
            inline=True,
        )
        embed.add_field(
            name="Trade Limits",
            value=f"{int(base_stats['trade_limits'])} -> **{int(final_stats['trade_limits'])}**",
            inline=True,
        )
        embed.add_field(
            name="Networth",
            value=f"${float(base_stats['networth']):.2f} -> **${float(final_stats['networth']):.2f}**",
            inline=True,
        )

        if matched:
            lines = []
            for row in matched[:20]:
                name = str(row.get("name", "Unknown"))
                stacks = int(row.get("stacks", 1))
                display = str(row.get("display", "")).strip()
                if display:
                    lines.append(f"- **{name}** (x{stacks}) · ({display})")
                else:
                    add = float(row.get("add", 0.0))
                    mul = float(row.get("mul", 1.0))
                    lines.append(
                        f"- **{name}** (x{stacks}) · (income {add:+.2f}, x{mul:.2f})"
                    )
            embed.add_field(name="Triggered Perks", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="Triggered Perks", value="None", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=False)
