from discord import Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_user
from stockbot.services.perks import evaluate_user_perks


def _effective_networth(guild_id: int, user_id: int, base_networth: float) -> float:
    result = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=0,
        base_networth=base_networth,
    )
    return float(result["final"]["networth"])


def setup_laugh(tree: app_commands.CommandTree) -> None:
    @tree.command(name="laugh", description="Laugh at a player with lower networth than you.")
    async def laugh(interaction: Interaction, target: Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        if target.id == interaction.user.id:
            await interaction.response.send_message(
                "You cannot laugh at yourself.",
                ephemeral=True,
            )
            return

        actor = get_user(interaction.guild.id, interaction.user.id)
        if actor is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return

        victim = get_user(interaction.guild.id, target.id)
        if victim is None:
            await interaction.response.send_message(
                f"{target.mention} is not registered.",
                ephemeral=True,
            )
            return

        actor_networth = _effective_networth(
            interaction.guild.id,
            interaction.user.id,
            float(actor.get("networth", 0.0)),
        )
        victim_networth = _effective_networth(
            interaction.guild.id,
            target.id,
            float(victim.get("networth", 0.0)),
        )
        if actor_networth <= victim_networth:
            await interaction.response.send_message(
                (
                    "You can only laugh at players with lower networth than you.\n"
                    f"Your networth: `${actor_networth:.2f}` | "
                    f"{target.display_name}: `${victim_networth:.2f}`"
                ),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"{interaction.user.mention} laughs at how poor {target.mention} is.",
            ephemeral=False,
        )
