from __future__ import annotations

import discord
from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Select, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_user
from stockbot.services.properties import (
    ascend_property,
    buy_property,
    get_properties,
    get_user_property_states,
    upgrade_property,
)


def _money(v: object) -> str:
    try:
        return f"${float(v):.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _state_by_property(states: list[dict]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for row in states:
        try:
            pid = int(row.get("property_id", 0))
        except (TypeError, ValueError):
            continue
        if pid > 0:
            out[pid] = row
    return out


def _format_effects(effects: list[dict]) -> str:
    if not effects:
        return "- None"
    lines: list[str] = []
    for eff in effects:
        target = str(eff.get("target_stat", "income"))
        mode = str(eff.get("value_mode", "flat"))
        val = float(eff.get("value", 0.0) or 0.0)
        if mode == "multiplier":
            lines.append(f"- {target}: x{val:.2f}")
        else:
            sign = "+" if val >= 0 else "-"
            lines.append(f"- {target}: {sign}{abs(val):.2f}")
    return "\n".join(lines)


def build_property_embed(guild_id: int, user_id: int, selected_property_id: int | None = None) -> tuple[Embed, list[dict], dict[int, dict], int | None]:
    props = get_properties(guild_id, enabled_only=False)
    states = _state_by_property(get_user_property_states(guild_id, user_id))
    selected = selected_property_id
    if selected is None and props:
        selected = int(props[0].get("id", 0))
    prop = next((p for p in props if int(p.get("id", 0)) == int(selected or 0)), None)

    embed = Embed(
        title="Property Dealer",
        description="Endgame property progression. Buy at Level 1, upgrade to Level 5, then optionally ascend to permanent buffs.",
    )
    if prop is None:
        embed.add_field(name="Status", value="No properties configured yet.", inline=False)
        return embed, props, states, None

    pid = int(prop.get("id", 0))
    state = states.get(pid)
    level = int(state.get("level", 0)) if state else 0
    ascended = int(state.get("ascended", 0)) > 0 if state else False
    max_level = max(1, int(prop.get("max_level", 5) or 5))
    level_costs = list(prop.get("level_costs", []))
    level_effects = list(prop.get("level_effects", []))
    current_effects = level_effects[level - 1] if 1 <= level <= len(level_effects) else []

    status = "Not owned"
    if ascended:
        status = "Ascended (permanent)"
    elif level > 0:
        status = f"Owned L{level}/{max_level}"

    embed.add_field(
        name=f"{prop.get('name', 'Property')} · {status}",
        value=str(prop.get("description", "") or "No description."),
        inline=False,
    )
    image_url = str(prop.get("image_url", "") or "").strip()
    if image_url:
        embed.set_image(url=image_url)
    embed.add_field(
        name="Pricing",
        value=(
            f"Buy: **{_money(prop.get('buy_price', 0.0))}**\n"
            f"Ascend: **{_money(prop.get('ascension_cost', 0.0))}**"
        ),
        inline=True,
    )
    next_upgrade_cost = 0.0
    if 0 < level < max_level:
        idx = level
        next_upgrade_cost = float(level_costs[idx] if idx < len(level_costs) else 0.0)
    embed.add_field(
        name="Progress",
        value=(
            f"Level: **{level}/{max_level}**\n"
            f"Next Upgrade: **{_money(next_upgrade_cost)}**"
        ),
        inline=True,
    )
    embed.add_field(name="Current Level Effects", value=_format_effects(current_effects), inline=False)
    if ascended:
        embed.add_field(
            name="Ascension Effects",
            value=_format_effects(list(prop.get("ascension_effects", []))),
            inline=False,
        )
    return embed, props, states, pid


class PropertySelect(Select):
    def __init__(self, guild_id: int, user_id: int, current_property_id: int | None, props: list[dict]):
        options = []
        for prop in props[:25]:
            pid = int(prop.get("id", 0))
            options.append(
                discord.SelectOption(
                    label=str(prop.get("name", f"Property {pid}"))[:100],
                    value=str(pid),
                    default=pid == int(current_property_id or 0),
                )
            )
        super().__init__(
            placeholder="Select property",
            min_values=1,
            max_values=1,
            options=options,
        )
        self._guild_id = guild_id
        self._user_id = user_id

    async def callback(self, interaction: Interaction) -> None:
        view = self.view
        if not isinstance(view, PropertyView):
            await interaction.response.defer()
            return
        try:
            view.selected_property_id = int(self.values[0])
        except (TypeError, ValueError):
            pass
        view.refresh()
        await interaction.response.edit_message(embed=view.embed, view=view)


class PropertyView(View):
    def __init__(self, guild_id: int, user_id: int, selected_property_id: int | None = None) -> None:
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.user_id = user_id
        self.selected_property_id = selected_property_id
        self.embed: Embed = Embed(title="Property Dealer")
        self.refresh()

    def refresh(self) -> None:
        self.clear_items()
        embed, props, _states, selected = build_property_embed(
            self.guild_id,
            self.user_id,
            self.selected_property_id,
        )
        self.embed = embed
        self.selected_property_id = selected
        if props:
            self.add_item(PropertySelect(self.guild_id, self.user_id, self.selected_property_id, props))
            # Re-add action buttons after clear_items().
            self.add_item(self.buy_btn)
            self.add_item(self.upgrade_btn)
            self.add_item(self.ascend_btn)

    async def _run_action(self, interaction: Interaction, fn) -> None:
        if self.selected_property_id is None:
            await interaction.response.send_message("No property selected.", ephemeral=True)
            return
        ok, msg = fn(self.guild_id, self.user_id, int(self.selected_property_id))
        self.refresh()
        await interaction.response.edit_message(embed=self.embed, view=self)
        await interaction.followup.send(msg, ephemeral=True)

    @button(label="Buy", style=ButtonStyle.green)
    async def buy_btn(self, interaction: Interaction, _button) -> None:
        await self._run_action(interaction, buy_property)

    @button(label="Upgrade", style=ButtonStyle.primary)
    async def upgrade_btn(self, interaction: Interaction, _button) -> None:
        await self._run_action(interaction, upgrade_property)

    @button(label="Ascend", style=ButtonStyle.danger)
    async def ascend_btn(self, interaction: Interaction, _button) -> None:
        await self._run_action(interaction, ascend_property)


async def send_property_panel(
    interaction: Interaction,
    guild_id: int,
    user_id: int,
    *,
    ephemeral: bool = True,
) -> None:
    view = PropertyView(guild_id, user_id)
    if interaction.response.is_done():
        await interaction.followup.send(embed=view.embed, view=view, ephemeral=ephemeral)
        return
    await interaction.response.send_message(embed=view.embed, view=view, ephemeral=ephemeral)


def setup_property(tree: app_commands.CommandTree) -> None:
    @tree.command(name="property", description="Open property dealer and manage your properties.")
    async def property_cmd(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        user = get_user(interaction.guild.id, interaction.user.id)
        if user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        await send_property_panel(
            interaction,
            interaction.guild.id,
            interaction.user.id,
            ephemeral=True,
        )
