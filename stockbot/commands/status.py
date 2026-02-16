from pathlib import Path

import discord
from discord import ButtonStyle, Embed, File, Interaction, Member, SelectOption, app_commands
from discord.ui import Button, Select, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config.runtime import get_app_config
from stockbot.db import (
    get_commodities,
    get_companies,
    get_user_commodities,
    get_user_status,
    get_users,
    recalc_user_networth,
)


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(hasattr(ui, name) for name in ("LayoutView", "Container", "TextDisplay", "ActionRow"))


class QuickPriceSelect(Select):
    def __init__(self, guild_id: int) -> None:
        rows = get_companies(guild_id)[:25]
        options = [
            SelectOption(
                label=f"{str(row.get('symbol', '')).upper()} · {str(row.get('name', 'Unknown'))[:70]}",
                value=str(row.get("symbol", "")).upper(),
            )
            for row in rows
            if str(row.get("symbol", "")).strip()
        ]
        if not options:
            options = [SelectOption(label="No companies available", value="")]
        super().__init__(
            placeholder="Select company for quick price view",
            min_values=1,
            max_values=1,
            options=options,
            disabled=not bool(rows),
        )
        self._guild_id = guild_id

    async def callback(self, interaction: Interaction) -> None:
        symbol = str(self.values[0]).upper().strip()
        if not symbol:
            await interaction.response.send_message("No companies available.", ephemeral=True)
            return
        from stockbot.commands.price import build_price_payload

        embed, chart_file, view, error = build_price_payload(
            guild_id=self._guild_id,
            user_id=interaction.user.id,
            symbol=symbol,
            last=None,
        )
        if error is not None:
            await interaction.response.send_message(error, ephemeral=True)
            return
        await interaction.response.send_message(
            embed=embed,
            file=chart_file,
            view=view,
            ephemeral=True,
        )


class QuickPriceSelectView(View):
    def __init__(self, owner_id: int, guild_id: int) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self.add_item(QuickPriceSelect(guild_id))

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can use this panel.",
                ephemeral=True,
            )
            return False
        return True


class QuickDescSelect(Select):
    def __init__(self, guild_id: int) -> None:
        companies = get_companies(guild_id)[:25]
        options = [
            SelectOption(
                label=f"{str(row.get('symbol', '')).upper()} · {str(row.get('name', 'Unknown'))[:70]}",
                value=f"company::{str(row.get('symbol', '')).upper()}",
            )
            for row in companies
            if str(row.get("symbol", "")).strip()
        ]
        if not options:
            options = [SelectOption(label="No companies available", value="")]
        super().__init__(
            placeholder="Select company lore to view",
            min_values=1,
            max_values=1,
            options=options,
            disabled=not bool(companies),
        )
        self._guild_id = guild_id

    async def callback(self, interaction: Interaction) -> None:
        value = str(self.values[0]).strip()
        if not value or "::" not in value:
            await interaction.response.send_message("No lore available.", ephemeral=True)
            return
        _kind, symbol = value.split("::", 1)
        from stockbot.commands.desc import DescPager

        rows = get_companies(self._guild_id)
        if not rows:
            await interaction.response.send_message("No companies found.", ephemeral=True)
            return
        idx = next(
            (i for i, row in enumerate(rows) if str(row.get("symbol", "")).upper() == symbol.upper()),
            0,
        )
        pager = DescPager(interaction.user.id, "companies", rows, idx)
        await interaction.response.send_message(embed=pager._build_embed(), view=pager, ephemeral=True)


class QuickDescSelectView(View):
    def __init__(self, owner_id: int, guild_id: int) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self.add_item(QuickDescSelect(guild_id))

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can use this panel.",
                ephemeral=True,
            )
            return False
        return True


class StatusQuickActionsHub(discord.ui.LayoutView):
    def __init__(self, owner_id: int, guild_id: int) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._guild_id = guild_id
        self._render()

    def _render(self) -> None:
        self.clear_items()
        ui = discord.ui
        container = ui.Container()
        container.add_item(
            ui.TextDisplay(
                content=(
                    "# Quick Actions\n"
                    "Use buttons below to access core gameplay flows without typing commands."
                )
            )
        )
        if hasattr(ui, "Separator"):
            container.add_item(ui.Separator())

        row_1 = ui.ActionRow()
        bank_btn = discord.ui.Button(label="Bank", style=ButtonStyle.primary)
        shop_btn = discord.ui.Button(label="Shop", style=ButtonStyle.green)
        list_btn = discord.ui.Button(label="List", style=ButtonStyle.secondary)

        row_2 = ui.ActionRow()
        price_btn = discord.ui.Button(label="Price", style=ButtonStyle.secondary)
        desc_btn = discord.ui.Button(label="Desc", style=ButtonStyle.secondary)
        ranking_btn = discord.ui.Button(label="Ranking", style=ButtonStyle.secondary)

        async def bank_cb(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message("Only the command user can use this panel.", ephemeral=True)
                return
            from stockbot.commands.bank import _build_bank_menu_v2_view

            view = _build_bank_menu_v2_view(self._guild_id, interaction.user.id)
            if view is not None:
                await interaction.response.send_message(view=view, ephemeral=True)
                return
            await interaction.response.send_message("Bank panel unavailable in this runtime. Use `/bank`.", ephemeral=True)

        async def shop_cb(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message("Only the command user can use this panel.", ephemeral=True)
                return
            from stockbot.commands.shop import ShopPager, _build_store

            rows = _build_store(self._guild_id)
            if not rows:
                await interaction.response.send_message("No commodities available yet.", ephemeral=True)
                return
            view = ShopPager(interaction.user.id, self._guild_id, rows)
            await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

        async def list_cb(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message("Only the command user can use this panel.", ephemeral=True)
                return
            companies = get_companies(self._guild_id)[:20]
            commodities = get_commodities(self._guild_id)[:20]
            text_lines = []
            if companies:
                text_lines.append("## Companies")
                text_lines.extend(
                    [
                        f"- **{str(r.get('symbol', '')).upper()}** · {str(r.get('name', 'Unknown'))} · ${float(r.get('current_price', r.get('base_price', 0.0))):.2f}"
                        for r in companies
                    ]
                )
            else:
                text_lines.append("## Companies\n_No companies found._")
            if hasattr(ui, "Separator"):
                text_lines.append("")
            if commodities:
                text_lines.append("## Commodities")
                text_lines.extend(
                    [
                        f"- **{str(r.get('name', 'Unknown'))}** · ${float(r.get('price', 0.0)):.2f} · {str(r.get('rarity', 'common')).title()}"
                        for r in commodities
                    ]
                )
            else:
                text_lines.append("## Commodities\n_No commodities found._")

            v = ui.LayoutView(timeout=300)
            c = ui.Container()
            c.add_item(ui.TextDisplay(content="\n".join(text_lines)))
            v.add_item(c)
            await interaction.response.send_message(view=v, ephemeral=True)

        async def price_cb(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message("Only the command user can use this panel.", ephemeral=True)
                return
            await interaction.response.send_message(
                "Select a company for quick price actions:",
                view=QuickPriceSelectView(interaction.user.id, self._guild_id),
                ephemeral=True,
            )

        async def desc_cb(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message("Only the command user can use this panel.", ephemeral=True)
                return
            await interaction.response.send_message(
                "Select a company to open lore description:",
                view=QuickDescSelectView(interaction.user.id, self._guild_id),
                ephemeral=True,
            )

        async def ranking_cb(interaction: Interaction) -> None:
            if interaction.user.id != self._owner_id:
                await interaction.response.send_message("Only the command user can use this panel.", ephemeral=True)
                return
            gm_id = int(get_app_config("GM_ID"))
            rows = get_users(self._guild_id)
            if gm_id > 0:
                rows = [row for row in rows if int(row.get("user_id", 0)) != gm_id]
            if not rows:
                await interaction.response.send_message("No players found.", ephemeral=True)
                return
            top = rows[:5]
            lines = []
            for idx, row in enumerate(top, start=1):
                uid = int(row.get("user_id", 0))
                name = str(row.get("display_name", "")).strip() or f"User {uid}"
                net = float(row.get("networth", 0.0))
                lines.append(f"**#{idx}** {name} — `${net:.2f}`")
            embed = Embed(title="Top 5 Networth", description="\n".join(lines))
            await interaction.response.send_message(embed=embed, ephemeral=True)

        bank_btn.callback = bank_cb
        shop_btn.callback = shop_cb
        list_btn.callback = list_cb
        price_btn.callback = price_cb
        desc_btn.callback = desc_cb
        ranking_btn.callback = ranking_cb

        row_1.add_item(bank_btn)
        row_1.add_item(shop_btn)
        row_1.add_item(list_btn)
        row_2.add_item(price_btn)
        row_2.add_item(desc_btn)
        row_2.add_item(ranking_btn)
        container.add_item(row_1)
        container.add_item(row_2)
        self.add_item(container)


class StatusQuickActionsEntryView(View):
    def __init__(self, owner_id: int, guild_id: int) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._guild_id = guild_id

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can use quick actions.",
                ephemeral=True,
            )
            return False
        return True

    @button(label="Quick Actions", style=ButtonStyle.primary)
    async def quick_actions(self, interaction: Interaction, _button: Button) -> None:
        if not _supports_components_v2():
            await interaction.response.send_message(
                "Quick Actions v2 is unavailable in this runtime.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            view=StatusQuickActionsHub(self._owner_id, self._guild_id),
            ephemeral=True,
        )


def setup_status(tree: app_commands.CommandTree) -> None:
    @tree.command(name="status", description="Show your status and holdings.")
    async def status(
        interaction: Interaction,
        member: Member | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=False,
            )
            return

        target = member or interaction.user
        recalc_user_networth(
            guild_id=interaction.guild.id,
            user_id=target.id,
        )
        data = get_user_status(
            guild_id=interaction.guild.id,
            user_id=target.id,
        )
        if data is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                ephemeral=True,
                view=RegisterNowView(),
            )
            return

        user = data["user"]
        holdings = data["holdings"]

        embed = Embed(
            title=f"📊 Status of {target.display_name}",
            description=f"{target.mention}"
        )
        balance = round(float(user["bank"]), 2)
        networth = round(float(user.get("networth", 0.0)), 2)
        owe = round(float(user.get("owe", 0.0)), 2)
        embed.add_field(name="💳 Balance", value=f"**${balance:.2f}**", inline=True)
        embed.add_field(name="💎 Networth", value=f"**${networth:.2f}**", inline=True)
        embed.add_field(name="🏦 Owe", value=f"**${owe:.2f}**", inline=True)
        embed.add_field(name="︽ Rank", value=str(user["rank"]), inline=True)
        embed.add_field(name="📅 Joined", value=user["joined_at"][:10], inline=True)

        if holdings:
            lines = [f"{row['symbol']}: {row['shares']}" for row in holdings]
            embed.add_field(name="📈 Holdings", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="📈 Holdings", value="None", inline=False)

        commodity_holdings = get_user_commodities(interaction.guild.id, target.id)
        used_commodities = sum(max(0, int(row.get("quantity", 0))) for row in commodity_holdings)
        commodity_limit = int(get_app_config("COMMODITIES_LIMIT"))
        if commodity_limit > 0:
            commodities_title = f"🪙 Commodities({used_commodities}/{commodity_limit})"
        else:
            commodities_title = f"🪙 Commodities({used_commodities}/∞)"
        if commodity_holdings:
            lines = []
            for row in commodity_holdings:
                qty = int(row["quantity"])
                price = float(row["price"])
                lines.append(f"{row['name']}: {qty} (${qty * price:.2f})")
            embed.add_field(name=commodities_title, value="\n".join(lines), inline=False)
        else:
            embed.add_field(name=commodities_title, value="None", inline=False)

        avatar_url = target.display_avatar.url
        embed.set_author(
            name=target.display_name,
            icon_url=avatar_url,
        )

        assets_dir = Path(__file__).resolve().parents[1] / "assets" / "images"
        banner_path = assets_dir / "Status.jpg"
        if banner_path.exists():
            banner = File(banner_path, filename="Status.jpg")
            embed.set_image(url="attachment://Status.jpg")
            await interaction.response.send_message(
                embed=embed,
                file=banner,
                view=StatusQuickActionsEntryView(interaction.user.id, interaction.guild.id),
            )
        else:
            await interaction.response.send_message(
                embed=embed,
                view=StatusQuickActionsEntryView(interaction.user.id, interaction.guild.id),
            )
