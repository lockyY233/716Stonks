from datetime import datetime, timezone

import discord
from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, Modal, TextInput, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import add_action_history, create_bank_request, get_user, repay_user_loan


class LoanRequestModal(Modal):
    def __init__(self, guild_id: int, user_id: int) -> None:
        super().__init__(title="Submit Loan Request")
        self._guild_id = guild_id
        self._user_id = user_id
        self.amount = TextInput(
            label="Loan amount",
            placeholder="Enter amount (e.g. 500)",
            required=True,
            max_length=32,
        )
        self.reason = TextInput(
            label="Reason",
            placeholder="Why do you need this loan?",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=800,
        )
        self.add_item(self.amount)
        self.add_item(self.reason)

    async def on_submit(self, interaction: Interaction) -> None:
        try:
            amount = round(float(self.amount.value), 2)
        except ValueError:
            await interaction.response.send_message(
                "Amount must be a number.",
                ephemeral=True,
            )
            return
        if amount <= 0:
            await interaction.response.send_message(
                "Amount must be greater than 0.",
                ephemeral=True,
            )
            return

        request_id = create_bank_request(
            guild_id=self._guild_id,
            user_id=self._user_id,
            request_type="loan",
            amount=amount,
            reason=self.reason.value.strip(),
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        await interaction.response.send_message(
            f"Loan request submitted. Request ID: `{request_id}` for `${amount:.2f}`.",
            ephemeral=True,
        )


class PayLoanModal(Modal):
    def __init__(self, guild_id: int, user_id: int) -> None:
        super().__init__(title="Pay Loan")
        self._guild_id = guild_id
        self._user_id = user_id
        self.amount = TextInput(
            label="Repayment amount",
            placeholder="Enter amount (e.g. 200) or type ALL",
            required=True,
            max_length=32,
        )
        self.add_item(self.amount)

    async def on_submit(self, interaction: Interaction) -> None:
        user = get_user(self._guild_id, self._user_id)
        if user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return

        raw = self.amount.value.strip().upper()
        if raw == "ALL":
            request_amount = max(
                0.0,
                min(float(user.get("bank", 0.0)), float(user.get("owe", 0.0))),
            )
        else:
            try:
                request_amount = round(float(raw), 2)
            except ValueError:
                await interaction.response.send_message(
                    "Amount must be a number or `ALL`.",
                    ephemeral=True,
                )
                return

        if request_amount <= 0:
            await interaction.response.send_message(
                "Repayment amount must be greater than 0.",
                ephemeral=True,
            )
            return

        result = repay_user_loan(
            guild_id=self._guild_id,
            user_id=self._user_id,
            amount=request_amount,
        )
        if result is None:
            await interaction.response.send_message(
                "Could not process repayment.",
                ephemeral=True,
            )
            return

        paid = float(result["paid"])
        bank_after = float(result["bank_after"])
        owe_after = float(result["owe_after"])
        if paid <= 0:
            await interaction.response.send_message(
                "No repayment made. Either your balance is 0 or you owe nothing.",
                ephemeral=True,
            )
            return

        add_action_history(
            self._guild_id,
            self._user_id,
            "loan_repayment",
            "bank",
            "LOAN",
            paid,
            1.0,
            paid,
            "Player loan repayment",
            datetime.now(timezone.utc).isoformat(),
        )
        await interaction.response.send_message(
            (
                f"Repayment successful: `${paid:.2f}`\n"
                f"New balance: `${bank_after:.2f}`\n"
                f"Remaining owe: `${owe_after:.2f}`"
            ),
            ephemeral=True,
        )


class LoanInfoView(View):
    def __init__(self, guild_id: int, user_id: int) -> None:
        super().__init__(timeout=300)
        self._guild_id = guild_id
        self._user_id = user_id

    @button(label="Submit Loan Request", style=ButtonStyle.green)
    async def submit_request(self, interaction: Interaction, _button: Button) -> None:
        await interaction.response.send_modal(LoanRequestModal(self._guild_id, self._user_id))


class BankMenuView(View):
    def __init__(self, guild_id: int, user_id: int) -> None:
        super().__init__(timeout=300)
        self._guild_id = guild_id
        self._user_id = user_id

    @button(label="Loan", style=ButtonStyle.blurple)
    async def loan(self, interaction: Interaction, _button: Button) -> None:
        embed = Embed(
            title="Bank Service: Loan",
            description=(
                "Loan requests are manually reviewed by the game master.\n"
                "If approved, funds are added to your balance and your `owe` value increases by the same amount."
            ),
        )
        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
            view=LoanInfoView(self._guild_id, self._user_id),
        )

    @button(label="Pay Loan", style=ButtonStyle.green)
    async def pay_loan(self, interaction: Interaction, _button: Button) -> None:
        await interaction.response.send_modal(PayLoanModal(self._guild_id, self._user_id))

    @button(label="More Services Soon", style=ButtonStyle.secondary, disabled=True)
    async def coming_soon(self, interaction: Interaction, _button: Button) -> None:
        await interaction.response.defer()


def setup_bank(tree: app_commands.CommandTree) -> None:
    @tree.command(name="bank", description="Open bank services.")
    async def bank(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        user = get_user(interaction.guild.id, interaction.user.id)
        if user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return

        embed = Embed(
            title="Bank Services",
            description="Choose a service below.",
        )
        await interaction.response.send_message(
            embed=embed,
            view=BankMenuView(interaction.guild.id, interaction.user.id),
            ephemeral=True,
        )
