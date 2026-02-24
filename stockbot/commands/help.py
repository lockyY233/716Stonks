from __future__ import annotations

import discord
from discord import ButtonStyle, Interaction, app_commands


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(hasattr(ui, name) for name in ("LayoutView", "Container", "TextDisplay", "ActionRow"))


HELP_SECTIONS: dict[str, str] = {
    "get_started": (
        "## Get Started\n"
        "1. `/register` to create your account.\n"
        "2. `/list target:companies` to see available stocks.\n"
        "3. `/price symbol:...` to view chart + actions.\n"
        "4. Use `/buy` and `/sell` to trade.\n"
        "5. Use `/shop` to buy commodities and build networth.\n"
        "6. Use `/ranking` to track the leaderboard."
    ),
    "tutorial": (
        "## Tutorial\n"
        "- Market updates every tick.\n"
        "- Stock prices are driven by base, slope, drift, and player flow.\n"
        "- Trading has per-window limits and fees.\n"
        "- Commodities increase networth and can trigger perks.\n"
        "- Use `/nextreset` to see when trading limits reset."
    ),
    "market": (
        "## Market Commands\n"
        "- `/price symbol:... [last:...]`\n"
        "- `/buy`\n"
        "- `/sell`\n"
        "- `/transactionhistory`\n"
        "- `/activity` (launch embedded activity in your voice channel)"
    ),
    "economy": (
        "## Economy Commands\n"
        "- `/shop`\n"
        "- `/slot`\n"
        "- `/steal member:...`\n"
        "- `/bank`\n"
        "- `/property`\n"
        "- `/donate target:... amount:...`\n"
        "- `/feedback`\n"
        "- `/nextreset`"
    ),
    "profile": (
        "## Profile Commands\n"
        "- `/status [member:...]`\n"
        "- `/ranking`\n"
        "- `/perks [member:...]`"
    ),
    "lookup": (
        "## Lookup Commands\n"
        "- `/list target:companies|commodities|players`\n"
        "- `/desc [target:...]`"
    ),
    "fun": (
        "## Fun Commands\n"
        "- `/hello`\n"
        "- `/no`"
    ),
}


class HelpV2View(discord.ui.LayoutView):
    def __init__(self, section: str = "get_started") -> None:
        super().__init__(timeout=300)
        self._section = section if section in HELP_SECTIONS else "get_started"
        self._render()

    def _render(self) -> None:
        self.clear_items()
        ui = discord.ui
        container = ui.Container()
        container.add_item(ui.TextDisplay(content="# 716Stonks Help"))
        container.add_item(
            ui.TextDisplay(
                content=(
                    "Public help panel. Choose a section below.\n"
                    "Admin-only commands are intentionally excluded."
                )
            )
        )
        if hasattr(ui, "Separator"):
            container.add_item(ui.Separator())
        container.add_item(ui.TextDisplay(content=HELP_SECTIONS[self._section]))

        top_row = ui.ActionRow()
        started_btn = discord.ui.Button(label="Get Started", style=ButtonStyle.success)
        tutorial_btn = discord.ui.Button(label="Tutorial", style=ButtonStyle.primary)

        async def _to_started(interaction: Interaction) -> None:
            self._section = "get_started"
            self._render()
            await interaction.response.edit_message(view=self)

        async def _to_tutorial(interaction: Interaction) -> None:
            self._section = "tutorial"
            self._render()
            await interaction.response.edit_message(view=self)

        started_btn.callback = _to_started
        tutorial_btn.callback = _to_tutorial
        top_row.add_item(started_btn)
        top_row.add_item(tutorial_btn)
        container.add_item(top_row)

        row_2 = ui.ActionRow()
        market_btn = discord.ui.Button(label="Market", style=ButtonStyle.secondary)
        economy_btn = discord.ui.Button(label="Economy", style=ButtonStyle.secondary)
        profile_btn = discord.ui.Button(label="Profile", style=ButtonStyle.secondary)
        lookup_btn = discord.ui.Button(label="Lookup", style=ButtonStyle.secondary)
        fun_btn = discord.ui.Button(label="Fun", style=ButtonStyle.secondary)

        async def _switch(interaction: Interaction, section: str) -> None:
            self._section = section
            self._render()
            await interaction.response.edit_message(view=self)

        market_btn.callback = lambda i: _switch(i, "market")
        economy_btn.callback = lambda i: _switch(i, "economy")
        profile_btn.callback = lambda i: _switch(i, "profile")
        lookup_btn.callback = lambda i: _switch(i, "lookup")
        fun_btn.callback = lambda i: _switch(i, "fun")

        row_2.add_item(market_btn)
        row_2.add_item(economy_btn)
        row_2.add_item(profile_btn)
        row_2.add_item(lookup_btn)
        row_2.add_item(fun_btn)
        container.add_item(row_2)

        self.add_item(container)


def setup_help(tree: app_commands.CommandTree) -> None:
    @tree.command(name="help", description="Show public help panel (non-admin commands).")
    async def help_command(interaction: Interaction) -> None:
        if not _supports_components_v2():
            await interaction.response.send_message(
                "Help requires Components V2 support in this runtime.",
                ephemeral=False,
            )
            return

        view = HelpV2View(section="get_started")
        await interaction.response.send_message(view=view, ephemeral=False)
