from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, View, button

from stockbot.db import get_companies


class DescPager(View):
    def __init__(
        self,
        owner_id: int,
        rows: list[dict],
        start_index: int,
    ) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
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
    @tree.command(name="desc", description="Show lore descriptions for companies.")
    @app_commands.describe(
        field="Select what to describe.",
        target="Company symbol (optional, jumps to that company page).",
    )
    @app_commands.choices(
        field=[
            app_commands.Choice(name="companies", value="companies"),
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

        if field != "companies":
            await interaction.response.send_message(
                f"Unknown field: `{field}`.",
            )
            return

        rows = get_companies(interaction.guild.id)
        if not rows:
            await interaction.response.send_message("No companies found.")
            return

        start_index = 0
        if target:
            target_symbol = target.strip().upper()
            start_index = next(
                (idx for idx, row in enumerate(rows) if str(row.get("symbol", "")).upper() == target_symbol),
                -1,
            )
            if start_index < 0:
                await interaction.response.send_message(
                    f"Company `{target_symbol}` not found.",
                )
                return

        view = DescPager(interaction.user.id, rows, start_index)
        await interaction.response.send_message(embed=view._build_embed(), view=view)
