import discord
from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, Modal, TextInput, View, button

from stockbot.commands.company_graphs import (
    build_player_impact_curve,
    build_price_projection_curve,
)
from stockbot.core.commodity_rarity import rarity_color
from stockbot.db import (
    get_commodities,
    get_companies,
    get_users,
    update_company_admin_fields,
    update_commodity_admin_fields,
    update_user_admin_fields,
)


def _parse_kv_updates(raw: str) -> tuple[dict[str, str], str | None]:
    updates: dict[str, str] = {}
    for idx, line in enumerate(raw.splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        if "=" not in text:
            return {}, f"Line {idx} missing '=': {line}"
        key, value = text.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            return {}, f"Line {idx} has empty field name."
        updates[key] = value
    if not updates:
        return {}, "No updates were provided."
    return updates, None


class AdminEditModal(Modal):
    def __init__(self, parent_view: "AdminShowPager") -> None:
        if parent_view.target == "users":
            title = "Edit User Fields"
        elif parent_view.target == "companies":
            title = "Edit Company Fields"
        else:
            title = "Edit Commodity Fields"
        super().__init__(title=title)
        self._parent_view = parent_view
        defaults = parent_view.current_edit_defaults()
        self.updates = TextInput(
            label="field=value (one per line)",
            style=discord.TextStyle.paragraph,
            required=True,
            default=defaults,
            max_length=2000,
        )
        self.add_item(self.updates)

    async def on_submit(self, interaction: Interaction) -> None:
        updates, error = _parse_kv_updates(self.updates.value)
        if error is not None:
            await interaction.response.send_message(error, ephemeral=True)
            return

        if self._parent_view.target == "users":
            row = self._parent_view.current_row()
            ok = update_user_admin_fields(
                guild_id=self._parent_view.guild.id,
                user_id=int(row["user_id"]),
                updates=updates,
            )
        elif self._parent_view.target == "companies":
            row = self._parent_view.current_row()
            ok = update_company_admin_fields(
                guild_id=self._parent_view.guild.id,
                symbol=str(row["symbol"]),
                updates=updates,
            )
        else:
            row = self._parent_view.current_row()
            ok = update_commodity_admin_fields(
                guild_id=self._parent_view.guild.id,
                name=str(row["name"]),
                updates=updates,
            )

        if not ok:
            await interaction.response.send_message(
                "No valid fields were updated. Check field names and values.",
                ephemeral=True,
            )
            return

        await self._parent_view.reload_rows()
        if self._parent_view.target == "companies":
            row = self._parent_view.current_row()
            symbol = str(row.get("symbol", "")).upper()
            slope = float(row.get("slope", 0.0))
            drift = float(row.get("drift", 0.0))
            liquidity = max(1.0, float(row.get("liquidity", 100.0)))
            impact_power = max(0.1, float(row.get("impact_power", 1.0)))
            current_price = float(row.get("current_price", row.get("base_price", 1.0)))
            impact_curve = build_player_impact_curve(
                symbol=symbol,
                slope=slope,
                drift=drift,
                liquidity=liquidity,
                impact_power=impact_power,
            )
            projection_curve = build_price_projection_curve(
                symbol=symbol,
                start_price=current_price,
                slope=slope,
                drift=drift,
                liquidity=liquidity,
                impact_power=impact_power,
            )
            impact_embed = Embed(
                title=f"{symbol} updated",
                description="Player impact curve preview.",
            )
            impact_embed.set_image(url=f"attachment://{symbol.lower()}_impact_curve.png")
            sim_embed = Embed(
                title=f"{symbol} simulated path",
                description="No-trade simulation from current price/slope/drift.",
            )
            sim_embed.set_image(url=f"attachment://{symbol.lower()}_projection_curve.png")
            await interaction.response.send_message(
                content="Record updated.",
                embeds=[impact_embed, sim_embed],
                files=[impact_curve, projection_curve],
                ephemeral=True,
            )
        else:
            await interaction.response.send_message("Record updated.", ephemeral=True)
        await self._parent_view.refresh_message()


class AdminShowPager(View):
    def __init__(self, owner_id: int, guild: discord.Guild, target: str) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self.guild = guild
        self.target = target
        self._rows: list[dict] = []
        self._index = 0
        self.message: discord.InteractionMessage | None = None
        self._sync_buttons()

    async def reload_rows(self) -> None:
        if self.target == "users":
            self._rows = get_users(self.guild.id)
        elif self.target == "companies":
            self._rows = get_companies(self.guild.id)
        else:
            self._rows = get_commodities(self.guild.id)
        if self._rows:
            self._index = max(0, min(self._index, len(self._rows) - 1))
        else:
            self._index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        has_rows = bool(self._rows)
        self.prev.disabled = (not has_rows) or self._index <= 0
        self.next.disabled = (not has_rows) or self._index >= len(self._rows) - 1
        self.edit.disabled = not has_rows

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can interact with this panel.",
                ephemeral=True,
            )
            return False
        return True

    def current_row(self) -> dict:
        return self._rows[self._index]

    def current_edit_defaults(self) -> str:
        row = self.current_row()
        if self.target == "users":
            return "\n".join(
                [
                    f"bank={float(row.get('bank', 0.0)):.2f}",
                    f"rank={row.get('rank', '')}",
                ]
            )
        if self.target == "commodities":
            return "\n".join(
                [
                    f"name={row.get('name', '')}",
                    f"price={float(row.get('price', 0.0)):.2f}",
                    f"rarity={row.get('rarity', 'common')}",
                    f"image_url={row.get('image_url', '')}",
                    f"description={row.get('description', '')}",
                ]
            )
        return "\n".join(
            [
                f"name={row.get('name', '')}",
                f"location={row.get('location', '')}",
                f"industry={row.get('industry', '')}",
                f"founded_year={int(row.get('founded_year', 2000))}",
                f"description={row.get('description', '')}",
                f"evaluation={row.get('evaluation', '')}",
                f"base_price={float(row.get('base_price', 0.0)):.2f}",
                f"slope={float(row.get('slope', 0.0)):.4f}",
                f"drift={float(row.get('drift', 0.0)):.4f}",
                f"liquidity={float(row.get('liquidity', 0.0)):.2f}",
                f"impact_power={float(row.get('impact_power', 1.0)):.3f}",
                f"current_price={float(row.get('current_price', 0.0)):.2f}",
            ]
        )

    async def _build_user_embed(self, row: dict) -> Embed:
        user_id = int(row["user_id"])
        member = self.guild.get_member(user_id)
        if member is None:
            try:
                member = await self.guild.fetch_member(user_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                member = None

        display_name = str(row.get("display_name") or (member.display_name if member else user_id))
        embed = Embed(
            title=f"adminshow: user {self._index + 1}/{len(self._rows)}",
            description=f"**{display_name}**",
        )
        if member is not None:
            embed.set_thumbnail(url=member.display_avatar.url)
        for key in ("user_id", "display_name", "bank", "networth", "rank", "joined_at"):
            value = row.get(key)
            if key in {"bank", "networth"} and value is not None:
                text = f"{float(value):.2f}"
            else:
                text = str(value)
            embed.add_field(name=key, value=text, inline=(key != "joined_at"))
        embed.set_footer(text=f"Page {self._index + 1}/{len(self._rows)}")
        return embed

    async def _build_company_embed(self, row: dict) -> Embed:
        symbol = str(row.get("symbol", ""))
        name = str(row.get("name", ""))
        embed = Embed(
            title=f"adminshow: company {self._index + 1}/{len(self._rows)}",
            description=f"**{symbol}** — {name}",
        )
        fields = [
            ("symbol", symbol),
            ("name", name),
            ("location", str(row.get("location", ""))),
            ("industry", str(row.get("industry", ""))),
            ("founded_year", str(int(row.get("founded_year", 2000)))),
            ("description", str(row.get("description", ""))),
            ("evaluation", str(row.get("evaluation", ""))),
            ("base_price", f"{float(row.get('base_price', 0.0)):.2f}"),
            ("current_price", f"{float(row.get('current_price', 0.0)):.2f}"),
            ("slope", f"{float(row.get('slope', 0.0)):.4f}"),
            ("drift", f"{float(row.get('drift', 0.0)):.4f}"),
            ("liquidity", f"{float(row.get('liquidity', 0.0)):.2f}"),
            ("impact_power", f"{float(row.get('impact_power', 1.0)):.3f}"),
            ("pending_buy", f"{float(row.get('pending_buy', 0.0)):.2f}"),
            ("pending_sell", f"{float(row.get('pending_sell', 0.0)):.2f}"),
            ("starting_tick", str(int(row.get("starting_tick", 0)))),
            ("last_tick", str(int(row.get("last_tick", 0)))),
            ("updated_at", str(row.get("updated_at", ""))),
        ]
        for key, value in fields:
            embed.add_field(
                name=key,
                value=value,
                inline=key not in {"name", "description", "evaluation", "updated_at"},
            )
        embed.set_footer(text=f"Page {self._index + 1}/{len(self._rows)}")
        return embed

    async def _build_commodity_embed(self, row: dict) -> Embed:
        name = str(row.get("name", ""))
        price = float(row.get("price", 0.0))
        rarity = str(row.get("rarity", "common"))
        image_url = str(row.get("image_url", "") or "")
        description = str(row.get("description", "") or "")
        embed = Embed(
            title=f"adminshow: commodity {self._index + 1}/{len(self._rows)}",
            description=f"**{name}**",
            color=rarity_color(rarity),
        )
        embed.add_field(name="name", value=name, inline=True)
        embed.add_field(name="price", value=f"{price:.2f}", inline=True)
        embed.add_field(name="rarity", value=rarity, inline=True)
        embed.add_field(name="image_url", value=image_url or "-", inline=False)
        embed.add_field(name="description", value=description or "-", inline=False)
        if image_url:
            embed.set_image(url=image_url)
        embed.set_footer(text=f"Page {self._index + 1}/{len(self._rows)}")
        return embed

    async def build_current_embed(self) -> Embed:
        if not self._rows:
            return Embed(title=f"adminshow: {self.target}", description="No rows found.")
        row = self.current_row()
        if self.target == "users":
            return await self._build_user_embed(row)
        if self.target == "companies":
            return await self._build_company_embed(row)
        return await self._build_commodity_embed(row)

    async def refresh_message(self) -> None:
        if self.message is None:
            return
        self._sync_buttons()
        embed = await self.build_current_embed()
        await self.message.edit(embed=embed, view=self)

    @button(label="Prev", style=ButtonStyle.secondary)
    async def prev(self, interaction: Interaction, _button: Button) -> None:
        self._index = max(0, self._index - 1)
        self._sync_buttons()
        embed = await self.build_current_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="Next", style=ButtonStyle.secondary)
    async def next(self, interaction: Interaction, _button: Button) -> None:
        self._index = min(len(self._rows) - 1, self._index + 1)
        self._sync_buttons()
        embed = await self.build_current_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="Edit", style=ButtonStyle.primary)
    async def edit(self, interaction: Interaction, _button: Button) -> None:
        await interaction.response.send_modal(AdminEditModal(self))


def setup_adminshow(tree: app_commands.CommandTree) -> None:
    @tree.command(name="adminshow", description="Admin: inspect/edit database rows.")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="Pick what data to show.")
    @app_commands.choices(
        target=[
            app_commands.Choice(name="users", value="users"),
            app_commands.Choice(name="companies", value="companies"),
            app_commands.Choice(name="commodities", value="commodities"),
        ]
    )
    async def adminshow(interaction: Interaction, target: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        if target not in {"users", "companies", "commodities"}:
            await interaction.response.send_message(
                f"Unknown target: `{target}`.",
                ephemeral=True,
            )
            return

        view = AdminShowPager(interaction.user.id, interaction.guild, target)
        await view.reload_rows()
        embed = await view.build_current_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()
