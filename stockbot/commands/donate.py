from discord import Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_user, update_user_bank


def setup_donate(tree: app_commands.CommandTree) -> None:
    @tree.command(name="donate", description="Donate money to another player.")
    @app_commands.describe(
        member="Player who will receive the donation.",
        amount="Amount of money to donate.",
    )
    async def donate(
        interaction: Interaction,
        member: Member,
        amount: float,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        if member.id == interaction.user.id:
            await interaction.response.send_message(
                "You cannot donate to yourself.",
                ephemeral=True,
            )
            return

        value = round(float(amount), 2)
        if value <= 0:
            await interaction.response.send_message(
                "Donation amount must be greater than 0.",
                ephemeral=True,
            )
            return

        donor = get_user(interaction.guild.id, interaction.user.id)
        if donor is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return

        recipient = get_user(interaction.guild.id, member.id)
        if recipient is None:
            await interaction.response.send_message(
                f"{member.mention} is not registered yet.",
                ephemeral=True,
            )
            return

        donor_balance = float(donor["bank"])
        if donor_balance < value:
            await interaction.response.send_message(
                f"Not enough funds. You have ${donor_balance:.2f}.",
                ephemeral=True,
            )
            return

        update_user_bank(
            interaction.guild.id,
            interaction.user.id,
            donor_balance - value,
        )
        update_user_bank(
            interaction.guild.id,
            member.id,
            float(recipient["bank"]) + value,
        )

        await interaction.response.send_message(
            f"{interaction.user.mention} donated **${value:.2f}** to {member.mention}.",
        )
