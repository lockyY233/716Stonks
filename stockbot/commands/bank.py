from datetime import datetime, timezone

import discord
from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, Modal, Select, TextInput, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import (
    add_action_history,
    create_bank_request,
    get_user,
    get_user_commodities,
    repay_user_loan,
)
from stockbot.services.trading import perform_pawn_commodity


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(
        hasattr(ui, name)
        for name in ("LayoutView", "Container", "TextDisplay")
    )


def _build_bank_menu_v2_view(guild_id: int, user_id: int) -> discord.ui.View | None:
    if not _supports_components_v2():
        return None
    ui = discord.ui
    try:
        view = ui.LayoutView(timeout=300)
        container = ui.Container()
        container.add_item(
            ui.TextDisplay(
                content=(
                    "## Bank Services\n"
                    "Choose a service below.\n\n"
                    "- **Loan**: submit request for GM approval.\n"
                    "- **Pay Loan**: repay part/all of your current owe.\n"
                    "- **Pawn**: sell owned commodity at pawn rate."
                )
            )
        )
        view.add_item(container)

        loan_btn = discord.ui.Button(label="Loan", style=ButtonStyle.blurple)
        pay_btn = discord.ui.Button(label="Pay Loan", style=ButtonStyle.green)
        pawn_btn = discord.ui.Button(label="Pawn", style=ButtonStyle.red)
        coming_btn = discord.ui.Button(
            label="More Services Soon",
            style=ButtonStyle.secondary,
            disabled=True,
        )

        async def loan_cb(interaction: Interaction) -> None:
            await _send_loan_info(interaction, guild_id, user_id)

        async def pay_cb(interaction: Interaction) -> None:
            await interaction.response.send_modal(PayLoanModal(guild_id, user_id))

        async def pawn_cb(interaction: Interaction) -> None:
            await _send_pawn_selector(interaction, guild_id, user_id)

        async def coming_cb(interaction: Interaction) -> None:
            await interaction.response.defer()

        loan_btn.callback = loan_cb
        pay_btn.callback = pay_cb
        pawn_btn.callback = pawn_cb
        coming_btn.callback = coming_cb

        # Keep interactive controls inside the V2 container bubble.
        if hasattr(ui, "ActionRow"):
            row = ui.ActionRow()
            row.add_item(loan_btn)
            row.add_item(pay_btn)
            row.add_item(pawn_btn)
            row.add_item(coming_btn)
            container.add_item(row)
        else:
            # If ActionRow is unavailable in this runtime, skip V2 path.
            return None

        return view
    except Exception:
        return None


async def _send_loan_info(interaction: Interaction, guild_id: int, user_id: int) -> None:
    if _supports_components_v2() and hasattr(discord.ui, "ActionRow"):
        ui = discord.ui
        try:
            view = ui.LayoutView(timeout=300)
            container = ui.Container()
            container.add_item(
                ui.TextDisplay(
                    content=(
                        "## Bank Service: Loan\n"
                        "Loan requests are manually reviewed by the game master.\n"
                        "If approved, funds are added to your balance and your `owe` value increases by the same amount."
                    )
                )
            )
            row = ui.ActionRow()
            submit_btn = discord.ui.Button(label="Submit Loan Request", style=ButtonStyle.green)

            async def submit_cb(btn_interaction: Interaction) -> None:
                await btn_interaction.response.send_modal(LoanRequestModal(guild_id, user_id))

            submit_btn.callback = submit_cb
            row.add_item(submit_btn)
            container.add_item(row)
            view.add_item(container)
            await interaction.response.send_message(view=view, ephemeral=True)
            return
        except discord.HTTPException:
            pass

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
        view=LoanInfoView(guild_id, user_id),
    )


async def _send_pawn_selector(interaction: Interaction, guild_id: int, user_id: int) -> None:
    owned = get_user_commodities(guild_id, user_id)
    if not owned:
        await interaction.response.send_message(
            "You do not own any commodities to pawn.",
            ephemeral=True,
        )
        return
    if _supports_components_v2() and hasattr(discord.ui, "ActionRow"):
        try:
            await interaction.response.send_message(
                view=PawnCommodityV2View(owned),
                ephemeral=True,
            )
            return
        except discord.HTTPException:
            pass
    view = PawnCommodityView(owned)
    await interaction.response.send_message(
        embed=view.build_embed(),
        view=view,
        ephemeral=True,
    )


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
        await _send_loan_info(interaction, self._guild_id, self._user_id)

    @button(label="Pay Loan", style=ButtonStyle.green)
    async def pay_loan(self, interaction: Interaction, _button: Button) -> None:
        await interaction.response.send_modal(PayLoanModal(self._guild_id, self._user_id))

    @button(label="Pawn", style=ButtonStyle.red)
    async def pawn(self, interaction: Interaction, _button: Button) -> None:
        await _send_pawn_selector(interaction, self._guild_id, self._user_id)

    @button(label="More Services Soon", style=ButtonStyle.secondary, disabled=True)
    async def coming_soon(self, interaction: Interaction, _button: Button) -> None:
        await interaction.response.defer()


class PawnCommoditySelect(Select):
    def __init__(self, owned_rows: list[dict]) -> None:
        options: list[discord.SelectOption] = []
        for row in owned_rows[:25]:
            name = str(row.get("name", "Unknown"))
            qty = int(row.get("quantity", 0))
            options.append(
                discord.SelectOption(
                    label=name[:100],
                    value=name,
                    description=f"Owned: {qty}",
                )
            )
        super().__init__(
            placeholder="Select commodity to pawn",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: Interaction) -> None:
        parent = self.view
        if parent is None or not hasattr(parent, "selected_name"):
            await interaction.response.defer()
            return
        parent.selected_name = self.values[0]
        if hasattr(parent, "pawn_one"):
            parent.pawn_one.disabled = False
        if hasattr(parent, "pawn_all"):
            parent.pawn_all.disabled = False
        if hasattr(parent, "on_selection_changed"):
            await parent.on_selection_changed(interaction)
            return
        if hasattr(parent, "build_embed"):
            await interaction.response.edit_message(embed=parent.build_embed(), view=parent)
            return
        await interaction.response.edit_message(view=parent)


class PawnCommodityView(View):
    def __init__(self, owned_rows: list[dict]) -> None:
        super().__init__(timeout=300)
        self.selected_name: str | None = None
        self._owned_rows = owned_rows
        self._owned_qty_by_name: dict[str, int] = {
            str(row.get("name", "")): int(row.get("quantity", 0)) for row in owned_rows
        }
        self.add_item(PawnCommoditySelect(owned_rows))

    def build_embed(self) -> Embed:
        description = "Choose a commodity in the dropdown, then click `Pawn 1` or `Pawn All`."
        if self.selected_name:
            qty = int(self._owned_qty_by_name.get(self.selected_name, 0))
            description = (
                f"Selected: **{self.selected_name}** (owned: {qty})\n"
                "Use `Pawn 1` or `Pawn All` below."
            )
        embed = Embed(
            title="Bank Service: Pawn",
            description=description,
            # Near-background color to hide the visible left accent stripe.
            color=discord.Color.from_rgb(43, 45, 49),
        )
        embed.add_field(
            name="Panel",
            value="Dropdown and buttons below are part of this pawn service.",
            inline=False,
        )
        return embed

    @button(label="Pawn 1", style=ButtonStyle.red, disabled=True)
    async def pawn_one(self, interaction: Interaction, _button: Button) -> None:
        if not self.selected_name:
            await interaction.response.send_message(
                "Select a commodity first.",
                ephemeral=True,
            )
            return
        ok, msg = await perform_pawn_commodity(interaction, self.selected_name, 1)
        if msg == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(
                msg,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(msg, ephemeral=True)

    @button(label="Pawn All", style=ButtonStyle.secondary, disabled=True)
    async def pawn_all(self, interaction: Interaction, _button: Button) -> None:
        if not self.selected_name:
            await interaction.response.send_message(
                "Select a commodity first.",
                ephemeral=True,
            )
            return
        qty = max(0, self._owned_qty_by_name.get(self.selected_name, 0))
        if qty <= 0:
            await interaction.response.send_message(
                "You no longer own this commodity.",
                ephemeral=True,
            )
            return
        ok, msg = await perform_pawn_commodity(interaction, self.selected_name, qty)
        if msg == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(
                msg,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(msg, ephemeral=True)


class PawnCommodityV2View(discord.ui.LayoutView):
    def __init__(self, owned_rows: list[dict]) -> None:
        super().__init__(timeout=300)
        self.selected_name: str | None = None
        self._owned_qty_by_name: dict[str, int] = {
            str(row.get("name", "")): int(row.get("quantity", 0)) for row in owned_rows
        }

        ui = discord.ui
        self.container = ui.Container()
        self.header = ui.TextDisplay(content=self._header_text())
        self.container.add_item(self.header)

        select_row = ui.ActionRow()
        self.select = PawnCommoditySelect(owned_rows)
        select_row.add_item(self.select)
        self.container.add_item(select_row)

        button_row = ui.ActionRow()
        self.pawn_one = discord.ui.Button(label="Pawn 1", style=ButtonStyle.red, disabled=True)
        self.pawn_all = discord.ui.Button(label="Pawn All", style=ButtonStyle.secondary, disabled=True)
        self.pawn_one.callback = self._pawn_one_callback
        self.pawn_all.callback = self._pawn_all_callback
        button_row.add_item(self.pawn_one)
        button_row.add_item(self.pawn_all)
        self.container.add_item(button_row)

        self.add_item(self.container)

    def _header_text(self) -> str:
        if self.selected_name:
            qty = int(self._owned_qty_by_name.get(self.selected_name, 0))
            return (
                "## Bank Service: Pawn\n"
                f"Selected: **{self.selected_name}** (owned: {qty})\n"
                "Use **Pawn 1** or **Pawn All** below."
            )
        return (
            "## Bank Service: Pawn\n"
            "Choose a commodity in the dropdown, then click **Pawn 1** or **Pawn All**."
        )

    async def on_selection_changed(self, interaction: Interaction) -> None:
        self.header.content = self._header_text()
        await interaction.response.edit_message(view=self)

    async def _pawn_one_callback(self, interaction: Interaction) -> None:
        if not self.selected_name:
            await interaction.response.send_message("Select a commodity first.", ephemeral=True)
            return
        ok, msg = await perform_pawn_commodity(interaction, self.selected_name, 1)
        if msg == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(
                msg,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(msg, ephemeral=True)

    async def _pawn_all_callback(self, interaction: Interaction) -> None:
        if not self.selected_name:
            await interaction.response.send_message("Select a commodity first.", ephemeral=True)
            return
        qty = max(0, self._owned_qty_by_name.get(self.selected_name, 0))
        if qty <= 0:
            await interaction.response.send_message("You no longer own this commodity.", ephemeral=True)
            return
        ok, msg = await perform_pawn_commodity(interaction, self.selected_name, qty)
        if msg == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(
                msg,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(msg, ephemeral=True)


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
        v2_view = _build_bank_menu_v2_view(interaction.guild.id, interaction.user.id)
        if v2_view is not None:
            try:
                await interaction.response.send_message(
                    view=v2_view,
                    ephemeral=True,
                )
                return
            except discord.HTTPException:
                # Runtime can expose V2 classes while rejecting a specific payload.
                # Fall back to classic UI for compatibility.
                pass

        await interaction.response.send_message(
            embed=embed,
            view=BankMenuView(interaction.guild.id, interaction.user.id),
            ephemeral=True,
        )
