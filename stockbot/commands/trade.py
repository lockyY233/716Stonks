from __future__ import annotations

from datetime import datetime, timezone

import discord
from discord import ButtonStyle, Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_user, get_user_commodities
from stockbot.services.perks import check_and_announce_perk_activations
from stockbot.services.trade_offers import (
    apply_trade_offer,
    create_trade_offer,
    get_trade_offer,
    save_trade_offer,
    validate_trade_offer,
)


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(hasattr(ui, name) for name in ("LayoutView", "Container", "TextDisplay", "ActionRow", "Separator"))


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _is_open_status(status: str) -> bool:
    return status in {"draft", "pending_target", "pending_requester"}


def _is_participant(offer: dict, user_id: int) -> bool:
    return user_id in {int(offer.get("requester_id", 0)), int(offer.get("target_id", 0))}


def _side_keys(offer: dict, user_id: int) -> tuple[str, str]:
    if int(user_id) == int(offer.get("requester_id", 0)):
        return "requester_money", "requester_items"
    return "target_money", "target_items"


def _keys_for_side(side: str) -> tuple[str, str]:
    if side == "requester":
        return "requester_money", "requester_items"
    return "target_money", "target_items"


def _side_user_id(offer: dict, side: str) -> int:
    if side == "requester":
        return int(offer.get("requester_id", 0))
    return int(offer.get("target_id", 0))


def _display_side(title: str, money: float, items: dict[str, int]) -> str:
    lines = [f"## {title}", "Money:", f"- **${float(money):.2f}**", "Commodities:"]
    if items:
        for name, qty in sorted(items.items(), key=lambda x: x[0].lower()):
            lines.append(f"- **{name} × {int(qty)}**")
    else:
        lines.append("- **None**")
    return "\n".join(lines)


def _status_text(offer: dict) -> str:
    st = str(offer.get("status", "draft"))
    if st == "draft":
        return "Draft (not sent)"
    if st == "pending_target":
        return f"Pending target (<@{int(offer.get('target_id', 0))}>)"
    if st == "pending_requester":
        return f"Pending requester (<@{int(offer.get('requester_id', 0))}>)"
    if st == "accepted":
        return "Accepted"
    if st == "denied":
        return "Denied"
    if st == "cancelled":
        return "Cancelled"
    if st == "expired":
        return "Expired"
    return st


def _sanitize_open_offer(offer: dict) -> tuple[dict, bool]:
    changed = False
    status = str(offer.get("status", "draft"))
    if _is_open_status(status):
        expires_at = int(offer.get("expires_at", 0) or 0)
        if expires_at > 0 and _now_epoch() > expires_at:
            offer["status"] = "expired"
            offer["pending_for"] = 0
            changed = True
    if changed:
        offer["updated_at"] = datetime.now(timezone.utc).isoformat()
    return offer, changed


def _trade_header(offer: dict) -> str:
    trade_id = str(offer.get("id", ""))
    created = str(offer.get("created_at", ""))[:19].replace("T", " ")
    expires = int(offer.get("expires_at", 0) or 0)
    remain = max(0, expires - _now_epoch()) if expires > 0 else 0
    remain_min = remain / 60.0
    return (
        f"# Trade Offer `{trade_id}`\n"
        f"Requester: <@{int(offer.get('requester_id', 0))}>  |  "
        f"Target: <@{int(offer.get('target_id', 0))}>\n"
        f"Status: **{_status_text(offer)}**\n"
        f"Created: `{created} UTC`  |  Expires in: `{remain_min:.1f} min`"
    )


def _build_trade_container(offer: dict) -> discord.ui.Container:
    ui = discord.ui
    container = ui.Container()
    container.add_item(ui.TextDisplay(content=_trade_header(offer)))
    container.add_item(ui.Separator())
    container.add_item(
        ui.TextDisplay(
            content=_display_side(
                f"Requester (<@{int(offer.get('requester_id', 0))}>) offers",
                float(offer.get("requester_money", 0.0)),
                dict(offer.get("requester_items", {})),
            )
        )
    )
    container.add_item(ui.Separator())
    container.add_item(
        ui.TextDisplay(
            content=_display_side(
                f"Target (<@{int(offer.get('target_id', 0))}>) offers",
                float(offer.get("target_money", 0.0)),
                dict(offer.get("target_items", {})),
            )
        )
    )
    return container


class TradeMoneyModal(discord.ui.Modal):
    amount = discord.ui.TextInput(label="Money amount to offer", placeholder="0", required=True, max_length=20)

    def __init__(self, trade_id: str, actor_id: int, edit_side: str) -> None:
        super().__init__(title="Edit Money Offer")
        self.trade_id = trade_id
        self.actor_id = actor_id
        self.edit_side = edit_side

    async def on_submit(self, interaction: Interaction) -> None:
        offer = get_trade_offer(self.trade_id)
        if offer is None:
            await interaction.response.send_message("Trade not found.", ephemeral=True)
            return
        offer, changed = _sanitize_open_offer(offer)
        if changed:
            save_trade_offer(offer)
        if not _is_participant(offer, self.actor_id):
            await interaction.response.send_message("Not part of this trade.", ephemeral=True)
            return
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("Only the selected trader can edit this side.", ephemeral=True)
            return
        try:
            value = max(0.0, float(str(self.amount.value).strip()))
        except (TypeError, ValueError):
            await interaction.response.send_message("Invalid amount.", ephemeral=True)
            return
        money_key, _items_key = _keys_for_side(self.edit_side)
        offer[money_key] = value
        save_trade_offer(offer)
        await interaction.response.send_message(
            view=TradeDraftView(self.trade_id, self.actor_id, notice="Money offer updated."),
            ephemeral=True,
        )


class TradeQtyModal(discord.ui.Modal):
    qty = discord.ui.TextInput(label="Quantity", placeholder="1", required=True, max_length=10)

    def __init__(self, trade_id: str, actor_id: int, commodity_name: str, edit_side: str) -> None:
        super().__init__(title=f"Offer {commodity_name}")
        self.trade_id = trade_id
        self.actor_id = actor_id
        self.commodity_name = commodity_name
        self.edit_side = edit_side

    async def on_submit(self, interaction: Interaction) -> None:
        offer = get_trade_offer(self.trade_id)
        if offer is None:
            await interaction.response.send_message("Trade not found.", ephemeral=True)
            return
        offer, changed = _sanitize_open_offer(offer)
        if changed:
            save_trade_offer(offer)
        if not _is_participant(offer, self.actor_id):
            await interaction.response.send_message("Not part of this trade.", ephemeral=True)
            return
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("Only the selected trader can edit this side.", ephemeral=True)
            return
        try:
            qty = int(str(self.qty.value).strip())
        except (TypeError, ValueError):
            await interaction.response.send_message("Quantity must be an integer.", ephemeral=True)
            return
        if qty <= 0:
            await interaction.response.send_message("Quantity must be > 0.", ephemeral=True)
            return
        side_user_id = _side_user_id(offer, self.edit_side)
        holdings = {str(r["name"]): int(r["quantity"]) for r in get_user_commodities(int(offer["guild_id"]), side_user_id)}
        if qty > int(holdings.get(self.commodity_name, 0)):
            await interaction.response.send_message("Not enough quantity available for that side.", ephemeral=True)
            return
        _money_key, items_key = _keys_for_side(self.edit_side)
        items = dict(offer.get(items_key, {}))
        items[self.commodity_name] = qty
        offer[items_key] = items
        save_trade_offer(offer)
        await interaction.response.send_message(
            view=TradeDraftView(self.trade_id, self.actor_id, notice="Commodity offer updated."),
            ephemeral=True,
        )


class TradeCommodityAddSelect(discord.ui.Select):
    def __init__(self, trade_id: str, actor_id: int, options_rows: list[dict], edit_side: str) -> None:
        opts = [
            discord.SelectOption(label=str(r["name"])[:90], value=str(r["name"]), description=f"Owned: {int(r['quantity'])}")
            for r in options_rows[:25]
        ]
        super().__init__(
            placeholder="Pick a commodity to offer",
            min_values=1,
            max_values=1,
            options=opts or [discord.SelectOption(label="No commodities", value="")],
            disabled=not bool(opts),
        )
        self.trade_id = trade_id
        self.actor_id = actor_id
        self.edit_side = edit_side

    async def callback(self, interaction: Interaction) -> None:
        value = str(self.values[0]).strip()
        if not value:
            await interaction.response.send_message("No commodities available.", ephemeral=True)
            return
        await interaction.response.send_modal(TradeQtyModal(self.trade_id, self.actor_id, value, self.edit_side))


class TradeCommodityAddView(discord.ui.View):
    def __init__(self, trade_id: str, actor_id: int, options_rows: list[dict], edit_side: str) -> None:
        super().__init__(timeout=300)
        self._actor_id = actor_id
        self.add_item(TradeCommodityAddSelect(trade_id, actor_id, options_rows, edit_side))

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._actor_id:
            await interaction.response.send_message("Only the selected trader can edit this side.", ephemeral=True)
            return False
        return True


class TradeCommodityRemoveSelect(discord.ui.Select):
    def __init__(self, trade_id: str, actor_id: int, offered_items: dict[str, int], edit_side: str) -> None:
        opts = [
            discord.SelectOption(label=f"{name} × {int(qty)}", value=name)
            for name, qty in sorted(offered_items.items(), key=lambda x: x[0].lower())[:25]
        ]
        super().__init__(
            placeholder="Pick offered commodity to remove",
            min_values=1,
            max_values=1,
            options=opts or [discord.SelectOption(label="No offered commodities", value="")],
            disabled=not bool(opts),
        )
        self.trade_id = trade_id
        self.actor_id = actor_id
        self.edit_side = edit_side

    async def callback(self, interaction: Interaction) -> None:
        value = str(self.values[0]).strip()
        if not value:
            await interaction.response.send_message("Nothing to remove.", ephemeral=True)
            return
        offer = get_trade_offer(self.trade_id)
        if offer is None:
            await interaction.response.send_message("Trade not found.", ephemeral=True)
            return
        _money_key, items_key = _keys_for_side(self.edit_side)
        items = dict(offer.get(items_key, {}))
        items.pop(value, None)
        offer[items_key] = items
        save_trade_offer(offer)
        await interaction.response.send_message(
            view=TradeDraftView(self.trade_id, self.actor_id, notice=f"Removed `{value}` from your offer."),
            ephemeral=True,
        )


class TradeCommodityRemoveView(discord.ui.View):
    def __init__(self, trade_id: str, actor_id: int, offered_items: dict[str, int], edit_side: str) -> None:
        super().__init__(timeout=300)
        self._actor_id = actor_id
        self.add_item(TradeCommodityRemoveSelect(trade_id, actor_id, offered_items, edit_side))

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._actor_id:
            await interaction.response.send_message("Only the selected trader can edit this side.", ephemeral=True)
            return False
        return True


async def _ping_pending_user(interaction: Interaction, offer: dict, note: str = "") -> None:
    if interaction.channel is None:
        return
    pending_for = int(offer.get("pending_for", 0))
    if pending_for <= 0:
        return
    trade_id = str(offer.get("id", ""))
    prefix = f"<@{pending_for}> Trade `{trade_id}` needs your response."
    text = f"{prefix} {note}".strip()
    try:
        await interaction.channel.send(text)
    except Exception:
        pass


class TradePublicView(discord.ui.LayoutView):
    def __init__(self, trade_id: str) -> None:
        super().__init__(timeout=3600)
        self.trade_id = trade_id
        self._render()

    def _render(self) -> None:
        self.clear_items()
        ui = discord.ui
        offer = get_trade_offer(self.trade_id)
        if offer is None:
            container = ui.Container()
            container.add_item(ui.TextDisplay(content="Trade no longer exists."))
            self.add_item(container)
            return
        offer, changed = _sanitize_open_offer(offer)
        if changed:
            save_trade_offer(offer)
        container = _build_trade_container(offer)
        st = str(offer.get("status", "draft"))
        row = ui.ActionRow()
        accept_btn = discord.ui.Button(label="Accept", style=ButtonStyle.success, disabled=not _is_open_status(st))
        deny_btn = discord.ui.Button(label="Deny", style=ButtonStyle.danger, disabled=not _is_open_status(st))
        counter_btn = discord.ui.Button(label="Counteroffer", style=ButtonStyle.primary, disabled=not _is_open_status(st))

        async def accept_cb(interaction: Interaction) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            cur, changed2 = _sanitize_open_offer(cur)
            if changed2:
                save_trade_offer(cur)
            if not _is_open_status(str(cur.get("status", ""))):
                await interaction.response.send_message("Trade is no longer open.", ephemeral=True)
                return
            if int(cur.get("pending_for", 0)) != int(interaction.user.id):
                await interaction.response.send_message("It is not your turn to respond.", ephemeral=True)
                return
            ok, message = apply_trade_offer(cur)
            if not ok:
                await interaction.response.send_message(message, ephemeral=True)
                return
            cur["status"] = "accepted"
            cur["pending_for"] = 0
            save_trade_offer(cur)
            self._render()
            await interaction.response.edit_message(view=self)
            try:
                await check_and_announce_perk_activations(interaction, int(cur.get("requester_id", 0)))
                await check_and_announce_perk_activations(interaction, int(cur.get("target_id", 0)))
            except Exception:
                pass
            await interaction.followup.send("Trade accepted and settled.")

        async def deny_cb(interaction: Interaction) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            cur, changed2 = _sanitize_open_offer(cur)
            if changed2:
                save_trade_offer(cur)
            if not _is_open_status(str(cur.get("status", ""))):
                await interaction.response.send_message("Trade is no longer open.", ephemeral=True)
                return
            if int(cur.get("pending_for", 0)) != int(interaction.user.id):
                await interaction.response.send_message("It is not your turn to respond.", ephemeral=True)
                return
            cur["status"] = "denied"
            cur["pending_for"] = 0
            save_trade_offer(cur)
            self._render()
            await interaction.response.edit_message(view=self)
            await interaction.followup.send("Trade denied.")

        async def counter_cb(interaction: Interaction) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            cur, changed2 = _sanitize_open_offer(cur)
            if changed2:
                save_trade_offer(cur)
            if not _is_open_status(str(cur.get("status", ""))):
                await interaction.response.send_message("Trade is no longer open.", ephemeral=True)
                return
            if int(cur.get("pending_for", 0)) != int(interaction.user.id):
                await interaction.response.send_message("It is not your turn to counter.", ephemeral=True)
                return
            await interaction.response.send_message(
                view=TradeDraftView(self.trade_id, interaction.user.id, notice="Edit your side, then submit counter."),
                ephemeral=True,
            )

        accept_btn.callback = accept_cb
        deny_btn.callback = deny_cb
        counter_btn.callback = counter_cb
        row.add_item(accept_btn)
        row.add_item(deny_btn)
        row.add_item(counter_btn)
        container.add_item(row)
        self.add_item(container)


class TradeDraftView(discord.ui.LayoutView):
    def __init__(self, trade_id: str, actor_id: int, notice: str = "") -> None:
        super().__init__(timeout=900)
        self.trade_id = trade_id
        self.actor_id = actor_id
        self.notice = str(notice).strip()
        self._render()

    def _submit_label(self, offer: dict) -> str:
        status = str(offer.get("status", "draft"))
        requester_id = int(offer.get("requester_id", 0))
        target_id = int(offer.get("target_id", 0))
        if status == "draft" and self.actor_id == requester_id:
            return "Confirm Offer"
        if status == "pending_target" and self.actor_id == target_id:
            return "Submit Counter"
        if status == "pending_requester" and self.actor_id == requester_id:
            return "Submit Counter"
        return "Submit"

    def _render(self) -> None:
        self.clear_items()
        ui = discord.ui
        offer = get_trade_offer(self.trade_id)
        if offer is None:
            container = ui.Container()
            container.add_item(ui.TextDisplay(content="Trade no longer exists."))
            self.add_item(container)
            return
        offer, changed = _sanitize_open_offer(offer)
        if changed:
            save_trade_offer(offer)
        if not _is_participant(offer, self.actor_id):
            container = ui.Container()
            container.add_item(ui.TextDisplay(content="You are not a participant in this trade."))
            self.add_item(container)
            return
        if self.notice:
            notice_container = ui.Container()
            notice_container.add_item(ui.TextDisplay(content=self.notice))
            self.add_item(notice_container)
        status_open = _is_open_status(str(offer.get("status", "")))
        submit_btn = discord.ui.Button(label=self._submit_label(offer), style=ButtonStyle.success, disabled=not status_open)
        cancel_btn = discord.ui.Button(label="Cancel", style=ButtonStyle.danger, disabled=not status_open)

        async def money_cb(interaction: Interaction, edit_side: str) -> None:
            await interaction.response.send_modal(TradeMoneyModal(self.trade_id, self.actor_id, edit_side))

        async def add_cb(interaction: Interaction, edit_side: str) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            rows = get_user_commodities(int(cur["guild_id"]), _side_user_id(cur, edit_side))
            if not rows:
                await interaction.response.send_message("No commodities available for this side.", ephemeral=True)
                return
            await interaction.response.send_message(
                "Pick a commodity to add, then set quantity.",
                view=TradeCommodityAddView(self.trade_id, self.actor_id, rows, edit_side),
                ephemeral=True,
            )

        async def remove_cb(interaction: Interaction, edit_side: str) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            _money_key, items_key = _keys_for_side(edit_side)
            offered_items = dict(cur.get(items_key, {}))
            if not offered_items:
                await interaction.response.send_message("No offered commodities to remove on this side.", ephemeral=True)
                return
            await interaction.response.send_message(
                "Pick offered commodity to remove.",
                view=TradeCommodityRemoveView(self.trade_id, self.actor_id, offered_items, edit_side),
                ephemeral=True,
            )

        async def submit_cb(interaction: Interaction) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            cur, changed2 = _sanitize_open_offer(cur)
            if changed2:
                save_trade_offer(cur)
            st = str(cur.get("status", "draft"))
            requester_id = int(cur.get("requester_id", 0))
            target_id = int(cur.get("target_id", 0))
            if st == "draft":
                if self.actor_id != requester_id:
                    await interaction.response.send_message("Only requester can confirm initial offer.", ephemeral=True)
                    return
                cur["status"] = "pending_target"
                cur["pending_for"] = target_id
            elif st == "pending_target":
                if self.actor_id != target_id:
                    await interaction.response.send_message("Only target can submit this counter.", ephemeral=True)
                    return
                cur["status"] = "pending_requester"
                cur["pending_for"] = requester_id
            elif st == "pending_requester":
                if self.actor_id != requester_id:
                    await interaction.response.send_message("Only requester can submit this counter.", ephemeral=True)
                    return
                cur["status"] = "pending_target"
                cur["pending_for"] = target_id
            else:
                await interaction.response.send_message("Trade is closed.", ephemeral=True)
                return
            ok, err = validate_trade_offer(cur)
            if not ok:
                cur["status"] = st
                await interaction.response.send_message(err, ephemeral=True)
                return
            save_trade_offer(cur)
            public_view = TradePublicView(self.trade_id)
            if interaction.channel is None:
                await interaction.response.send_message("Cannot publish trade in this channel.", ephemeral=True)
                return
            try:
                if int(cur.get("message_id", 0)) > 0:
                    msg = await interaction.channel.fetch_message(int(cur["message_id"]))
                    await msg.edit(view=public_view)
                else:
                    msg = await interaction.channel.send(view=public_view)
                    cur["message_channel_id"] = int(interaction.channel.id)
                    cur["message_id"] = int(msg.id)
                    save_trade_offer(cur)
            except Exception:
                await interaction.response.send_message("Failed to publish trade message.", ephemeral=True)
                return
            await _ping_pending_user(interaction, cur)
            await interaction.response.send_message("Trade submitted.", ephemeral=True)

        async def cancel_cb(interaction: Interaction) -> None:
            cur = get_trade_offer(self.trade_id)
            if cur is None:
                await interaction.response.send_message("Trade not found.", ephemeral=True)
                return
            if int(cur.get("requester_id", 0)) != self.actor_id:
                await interaction.response.send_message("Only requester can cancel the trade.", ephemeral=True)
                return
            cur["status"] = "cancelled"
            cur["pending_for"] = 0
            save_trade_offer(cur)
            if interaction.channel is not None and int(cur.get("message_id", 0)) > 0:
                try:
                    msg = await interaction.channel.fetch_message(int(cur["message_id"]))
                    await msg.edit(view=TradePublicView(self.trade_id))
                except Exception:
                    pass
            await interaction.response.send_message("Trade cancelled.", ephemeral=True)

        requester_money_btn = discord.ui.Button(label="Requester Money", style=ButtonStyle.primary, disabled=not status_open)
        requester_add_btn = discord.ui.Button(label="Requester Add", style=ButtonStyle.primary, disabled=not status_open)
        requester_remove_btn = discord.ui.Button(label="Requester Remove", style=ButtonStyle.primary, disabled=not status_open)
        target_money_btn = discord.ui.Button(label="Receiver Money", style=ButtonStyle.primary, disabled=not status_open)
        target_add_btn = discord.ui.Button(label="Receiver Add", style=ButtonStyle.primary, disabled=not status_open)
        target_remove_btn = discord.ui.Button(label="Receiver Remove", style=ButtonStyle.primary, disabled=not status_open)

        async def requester_money_cb(interaction: Interaction) -> None:
            await money_cb(interaction, "requester")

        async def requester_add_cb(interaction: Interaction) -> None:
            await add_cb(interaction, "requester")

        async def requester_remove_cb(interaction: Interaction) -> None:
            await remove_cb(interaction, "requester")

        async def target_money_cb(interaction: Interaction) -> None:
            await money_cb(interaction, "target")

        async def target_add_cb(interaction: Interaction) -> None:
            await add_cb(interaction, "target")

        async def target_remove_cb(interaction: Interaction) -> None:
            await remove_cb(interaction, "target")

        requester_money_btn.callback = requester_money_cb
        requester_add_btn.callback = requester_add_cb
        requester_remove_btn.callback = requester_remove_cb
        target_money_btn.callback = target_money_cb
        target_add_btn.callback = target_add_cb
        target_remove_btn.callback = target_remove_cb
        submit_btn.callback = submit_cb
        cancel_btn.callback = cancel_cb

        container = ui.Container()
        container.add_item(ui.TextDisplay(content=_trade_header(offer)))
        container.add_item(
            ui.TextDisplay(
                content=_display_side(
                    f"Requester (<@{int(offer.get('requester_id', 0))}>) offers",
                    float(offer.get("requester_money", 0.0)),
                    dict(offer.get("requester_items", {})),
                )
            )
        )
        requester_row = ui.ActionRow()
        requester_row.add_item(requester_money_btn)
        requester_row.add_item(requester_add_btn)
        requester_row.add_item(requester_remove_btn)
        container.add_item(requester_row)
        container.add_item(ui.Separator())
        container.add_item(
            ui.TextDisplay(
                content=_display_side(
                    f"Target (<@{int(offer.get('target_id', 0))}>) offers",
                    float(offer.get("target_money", 0.0)),
                    dict(offer.get("target_items", {})),
                )
            )
        )
        target_row = ui.ActionRow()
        target_row.add_item(target_money_btn)
        target_row.add_item(target_add_btn)
        target_row.add_item(target_remove_btn)
        container.add_item(target_row)
        container.add_item(ui.Separator())
        submit_row = ui.ActionRow()
        submit_row.add_item(submit_btn)
        submit_row.add_item(cancel_btn)
        container.add_item(submit_row)
        self.add_item(container)

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("Only this trade participant can use this draft panel.", ephemeral=True)
            return False
        return True


def setup_trade(tree: app_commands.CommandTree) -> None:
    @tree.command(name="trade", description="Create a commodity/money trade offer to another player.")
    async def trade(interaction: Interaction, target: Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        if not _supports_components_v2():
            await interaction.response.send_message("Trade requires Components v2 support in this runtime.", ephemeral=True)
            return
        if target.bot:
            await interaction.response.send_message("You can only trade with real players.", ephemeral=True)
            return
        if target.id == interaction.user.id:
            await interaction.response.send_message("You cannot trade with yourself.", ephemeral=True)
            return
        requester_user = get_user(interaction.guild.id, interaction.user.id)
        target_user = get_user(interaction.guild.id, target.id)
        if requester_user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        if target_user is None:
            await interaction.response.send_message("Target user must register first.", ephemeral=True)
            return
        offer = create_trade_offer(interaction.guild.id, interaction.user.id, target.id, ttl_seconds=3600)
        await interaction.response.send_message(
            view=TradeDraftView(
                str(offer["id"]),
                interaction.user.id,
                notice="Build your offer, then press Confirm Offer to publish it.",
            ),
            ephemeral=True,
        )
