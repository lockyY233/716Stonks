from __future__ import annotations

import math
from datetime import datetime

import discord
from discord import ButtonStyle, Interaction, app_commands

from stockbot.db import get_action_history

_PAGE_SIZE = 10


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(hasattr(ui, name) for name in ("LayoutView", "Container", "TextDisplay"))


def _fmt_dt(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return "-"
    # Requested format: MM/DD - HR:S
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%m/%d - %H:%S")
    except ValueError:
        return text


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def _fmt_transaction_row(index: int, row: dict) -> str:
    display_name = str(row.get("display_name", "")).strip() or f"U{int(row.get('user_id', 0))}"
    action_type = str(row.get("action_type", "")).strip().upper() or "-"
    target = str(row.get("target_symbol", "")).strip() or "-"
    quantity = float(row.get("quantity", 0.0))
    total_amount = float(row.get("total_amount", 0.0))
    unit_price = float(row.get("unit_price", 0.0))
    created_at = _fmt_dt(str(row.get("created_at", "")))
    return (
        f"{index:>2} "
        f"{created_at:<13} "
        f"{_truncate(display_name, 12):<12} "
        f"{_truncate(action_type, 6):<6} "
        f"{_truncate(target, 6):<6} "
        f"{quantity:>6.2f} "
        f"{unit_price:>8.2f} "
        f"{total_amount:>9.2f}"
    )


class TransactionHistoryView(discord.ui.LayoutView):
    def __init__(self, owner_id: int, rows: list[dict], page: int = 0) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._rows = rows
        self._page = max(0, page)
        self._total_pages = max(1, math.ceil(len(rows) / _PAGE_SIZE))
        self._render()

    def _render(self) -> None:
        self.clear_items()
        ui = discord.ui
        container = ui.Container()
        start = self._page * _PAGE_SIZE
        end = start + _PAGE_SIZE
        page_rows = self._rows[start:end]

        header = (
            f"# Recent Transactions\n"
            f"Page {self._page + 1}/{self._total_pages} · Showing {len(page_rows)} of {len(self._rows)}"
        )
        container.add_item(ui.TextDisplay(content=header))
        if not page_rows:
            container.add_item(ui.TextDisplay(content="_No transaction history found._"))
        else:
            table_header = " DATE          USER         ACTION TGT       QTY     UNIT     TOTAL"
            container.add_item(ui.TextDisplay(content=table_header))
            if hasattr(ui, "Separator"):
                container.add_item(ui.Separator())
            for i, row in enumerate(page_rows, start=start + 1):
                container.add_item(ui.TextDisplay(content=_fmt_transaction_row(i, row)))
                if hasattr(ui, "Separator") and i < start + len(page_rows):
                    container.add_item(ui.Separator())

        row = ui.ActionRow()
        prev_btn = discord.ui.Button(
            label="Prev",
            style=ButtonStyle.secondary,
            disabled=self._page <= 0,
        )
        next_btn = discord.ui.Button(
            label="Next",
            style=ButtonStyle.secondary,
            disabled=self._page >= self._total_pages - 1,
        )

        async def _prev(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message(
                    "Only the command user can change pages.",
                    ephemeral=True,
                )
                return
            self._page = max(0, self._page - 1)
            self._render()
            await interaction.response.edit_message(view=self)

        async def _next(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message(
                    "Only the command user can change pages.",
                    ephemeral=True,
                )
                return
            self._page = min(self._total_pages - 1, self._page + 1)
            self._render()
            await interaction.response.edit_message(view=self)

        prev_btn.callback = _prev
        next_btn.callback = _next
        row.add_item(prev_btn)
        row.add_item(next_btn)
        container.add_item(row)
        self.add_item(container)


def setup_transactionhistory(tree: app_commands.CommandTree) -> None:
    @tree.command(name="transactionhistory", description="Show recent market transactions (10 per page).")
    async def transactionhistory(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        if not _supports_components_v2() or not hasattr(discord.ui, "ActionRow"):
            await interaction.response.send_message(
                "V2 components are not available in the current runtime.",
                ephemeral=True,
            )
            return

        rows = get_action_history(interaction.guild.id, limit=500)
        view = TransactionHistoryView(interaction.user.id, rows, page=0)
        await interaction.response.send_message(view=view)
