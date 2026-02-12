from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import View, button

from stockbot.db import get_commodities, get_companies, get_users


_PAGE_SIZE = 10


class CompaniesPager(View):
    def __init__(self, owner_id: int, pages: list[Embed]) -> None:
        super().__init__(timeout=180)
        self._owner_id = owner_id
        self._pages = pages
        self._index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        self.prev.disabled = self._index <= 0
        self.next.disabled = self._index >= len(self._pages) - 1

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can change pages.",
                ephemeral=True,
            )
            return False
        return True

    @button(label="Prev", style=ButtonStyle.secondary)
    async def prev(self, interaction: Interaction, _button) -> None:
        self._index = max(0, self._index - 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._pages[self._index], view=self)

    @button(label="Next", style=ButtonStyle.secondary)
    async def next(self, interaction: Interaction, _button) -> None:
        self._index = min(len(self._pages) - 1, self._index + 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._pages[self._index], view=self)


def _build_pages(companies: list[dict]) -> list[Embed]:
    if not companies:
        return [
            Embed(
                title="Companies",
                description="No companies found yet.",
            )
        ]

    pages: list[Embed] = []
    total = len(companies)
    for start in range(0, total, _PAGE_SIZE):
        chunk = companies[start : start + _PAGE_SIZE]
        lines = []
        for company in chunk:
            symbol = company.get("symbol", "").upper()
            name = company.get("name") or "Unknown"
            price = company.get("current_price")
            if price is None:
                price = company.get("base_price")
            price_text = f"${price:.2f}" if price is not None else "N/A"
            lines.append(f"**{symbol}** — {name} ({price_text})")

        page_num = len(pages) + 1
        total_pages = (total + _PAGE_SIZE - 1) // _PAGE_SIZE
        embed = Embed(
            title="Companies",
            description="\n".join(lines),
        )
        embed.set_footer(text=f"Page {page_num}/{total_pages} • Total {total}")
        pages.append(embed)

    return pages


def _build_player_pages(players: list[dict]) -> list[Embed]:
    if not players:
        return [
            Embed(
                title="Players",
                description="No registered players found yet.",
            )
        ]

    pages: list[Embed] = []
    total = len(players)
    for start in range(0, total, _PAGE_SIZE):
        chunk = players[start : start + _PAGE_SIZE]
        lines = []
        for i, player in enumerate(chunk, start=start + 1):
            user_id = int(player["user_id"])
            bank = float(player["bank"])
            display_name = player.get("display_name", "")
            label = display_name if display_name else f"<@{user_id}>"
            networth = float(player.get("networth", 0.0))
            lines.append(f"**#{i}** {label} — `${bank:.2f}` | Networth `${networth:.2f}`")

        page_num = len(pages) + 1
        total_pages = (total + _PAGE_SIZE - 1) // _PAGE_SIZE
        embed = Embed(
            title="Players",
            description="\n".join(lines),
        )
        embed.set_footer(text=f"Page {page_num}/{total_pages} • Total {total}")
        pages.append(embed)

    return pages


def _build_commodities_pages(commodities: list[dict]) -> list[Embed]:
    if not commodities:
        return [
            Embed(
                title="Commodities",
                description="No commodities found yet.",
            )
        ]

    pages: list[Embed] = []
    total = len(commodities)
    for start in range(0, total, _PAGE_SIZE):
        chunk = commodities[start : start + _PAGE_SIZE]
        lines = []
        for commodity in chunk:
            name = str(commodity.get("name", "Unknown"))
            price = float(commodity.get("price", 0.0))
            rarity = str(commodity.get("rarity", "common")).title()
            lines.append(f"**{name}** — ${price:.2f} ({rarity})")

        page_num = len(pages) + 1
        total_pages = (total + _PAGE_SIZE - 1) // _PAGE_SIZE
        embed = Embed(
            title="Commodities",
            description="\n".join(lines),
        )
        embed.set_footer(text=f"Page {page_num}/{total_pages} • Total {total}")
        pages.append(embed)

    return pages


def setup_list(tree: app_commands.CommandTree) -> None:
    @tree.command(name="list", description="List available companies.")
    @app_commands.describe(target="Pick a list to show.")
    @app_commands.choices(
        target=[
            app_commands.Choice(name="companies", value="companies"),
            app_commands.Choice(name="commodities", value="commodities"),
            app_commands.Choice(name="players", value="players"),
        ]
    )
    async def list_companies(
        interaction: Interaction,
        target: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return
        if target == "companies":
            rows = get_companies(interaction.guild.id)
            pages = _build_pages(rows)
            view = CompaniesPager(interaction.user.id, pages)
            await interaction.response.send_message(embed=pages[0], view=view)
            return
        if target == "players":
            rows = get_users(interaction.guild.id)
            pages = _build_player_pages(rows)
            view = CompaniesPager(interaction.user.id, pages)
            await interaction.response.send_message(embed=pages[0], view=view)
            return
        if target == "commodities":
            rows = get_commodities(interaction.guild.id)
            pages = _build_commodities_pages(rows)
            view = CompaniesPager(interaction.user.id, pages)
            await interaction.response.send_message(embed=pages[0], view=view)
            return

        await interaction.response.send_message(
            f"Unknown list target: `{target}`.",
            ephemeral=True,
        )
