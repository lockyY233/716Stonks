from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, View, button

from stockbot.core.commodity_rarity import rarity_color
from stockbot.db import get_commodities, get_companies


class DescPager(View):
    def __init__(
        self,
        owner_id: int,
        target_kind: str,
        rows: list[dict],
        start_index: int,
    ) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._target_kind = target_kind
        self._rows = rows
        self._index = max(0, min(start_index, len(rows) - 1))
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        self.prev.disabled = self._index <= 0
        self.next.disabled = self._index >= len(self._rows) - 1

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can interact with this panel.",
                ephemeral=True,
            )
            return False
        return True

    def _build_embed(self) -> Embed:
        row = self._rows[self._index]
        if self._target_kind == "commodities":
            name = str(row.get("name", "Unknown"))
            price = float(row.get("price", 0.0))
            rarity = str(row.get("rarity", "common"))
            image_url = str(row.get("image_url", "") or "")
            description = str(row.get("description", "") or "No description.")
            embed = Embed(
                title=name,
                description=description,
                color=rarity_color(rarity),
            )
            embed.add_field(name="Price", value=f"${price:.2f}", inline=True)
            embed.add_field(name="Rarity", value=rarity.title(), inline=True)
            if image_url:
                embed.set_image(url=image_url)
            embed.set_footer(text=f"Page {self._index + 1}/{len(self._rows)}")
            return embed

        name = str(row.get("name", row.get("symbol", "")))
        symbol = str(row.get("symbol", ""))
        industry = str(row.get("industry", "") or "Unknown")
        founded_year = str(row.get("founded_year", "") or "Unknown")
        location = str(row.get("location", "") or "Unknown")
        evaluation = str(row.get("evaluation", "") or "Unknown")
        description = str(row.get("description", "") or "No lore description.")

        embed = Embed(
            title=f"{name} ({symbol})",
            description=description,
        )
        embed.add_field(name="Industry", value=industry, inline=True)
        embed.add_field(name="Founded", value=founded_year, inline=True)
        embed.add_field(name="Location", value=location, inline=True)
        embed.add_field(name="Evaluation", value=evaluation, inline=True)
        embed.set_footer(text=f"Page {self._index + 1}/{len(self._rows)}")
        return embed

    @button(label="Prev", style=ButtonStyle.secondary)
    async def prev(self, interaction: Interaction, _button: Button) -> None:
        self._index = max(0, self._index - 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @button(label="Next", style=ButtonStyle.secondary)
    async def next(self, interaction: Interaction, _button: Button) -> None:
        self._index = min(len(self._rows) - 1, self._index + 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)


def setup_desc(tree: app_commands.CommandTree) -> None:
    @tree.command(name="desc", description="Show lore descriptions for companies and commodities.")
    @app_commands.describe(
        field="Select what to describe.",
        target="Optional target (company symbol or commodity name).",
    )
    @app_commands.choices(
        field=[
            app_commands.Choice(name="companies", value="companies"),
            app_commands.Choice(name="commodities", value="commodities"),
        ]
    )
    async def desc(
        interaction: Interaction,
        field: str,
        target: str | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
            )
            return

        if field not in {"companies", "commodities"}:
            await interaction.response.send_message(
                f"Unknown field: `{field}`.",
            )
            return

        if field == "companies":
            rows = get_companies(interaction.guild.id)
        else:
            rows = get_commodities(interaction.guild.id)

        if not rows:
            await interaction.response.send_message(
                "No companies found." if field == "companies" else "No commodities found."
            )
            return

        start_index = 0
        if target:
            target_key = target.strip()
            if field == "companies":
                target_symbol = target_key.upper()
                start_index = next(
                    (idx for idx, row in enumerate(rows) if str(row.get("symbol", "")).upper() == target_symbol),
                    -1,
                )
            else:
                target_name = target_key.lower()
                start_index = next(
                    (idx for idx, row in enumerate(rows) if str(row.get("name", "")).lower() == target_name),
                    -1,
                )
            if start_index < 0:
                await interaction.response.send_message(
                    (
                        f"Company `{target_key.upper()}` not found."
                        if field == "companies"
                        else f"Commodity `{target_key}` not found."
                    ),
                )
                return

        view = DescPager(interaction.user.id, field, rows, start_index)
        await interaction.response.send_message(embed=view._build_embed(), view=view)
