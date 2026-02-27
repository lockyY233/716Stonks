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
        "2. `/list target:companies` and `/price symbol:...` to scout stocks.\n"
        "3. `/buy` and `/sell` to trade shares.\n"
        "4. `/shop`, `/jobs`, `/bank`, `/property` for economy progression.\n"
        "5. `/status`, `/income`, `/perks`, `/ranking` to track your progress."
    ),
    "tutorial": (
        "## Gameplay Loop\n"
        "- **Goal**: Climb `/ranking` by growing networth.\n"
        "- Income sources: stock trading, jobs, slot, rank income, and perks.\n"
        "- Commodities and properties are long-term progression systems.\n"
        "- Use `/nextreset` to track trading reset windows."
    ),
    "market": (
        "## Market Commands\n"
        "- `/list target:companies|players`\n"
        "- `/price symbol:... [last:...]`\n"
        "- `/buy`\n"
        "- `/sell`\n"
        "- `/transactionhistory`"
    ),
    "economy": (
        "## Economy Commands\n"
        "- `/shop`\n"
        "- `/bank`\n"
        "- `/pawn`\n"
        "- `/jobs`\n"
        "- `/property`\n"
        "- `/slot`\n"
        "- `/steal`"
    ),
    "alerts": (
        "## Alerts & Transfers\n"
        "- `/notify add`\n"
        "- `/notify list`\n"
        "- `/notify remove`\n"
        "- `/notify clear`\n"
        "- `/trade member:...`\n"
        "- `/donate target:... amount:...`\n"
        "- `/feedback`"
    ),
    "profile": (
        "## Profile Commands\n"
        "- `/status [member:...]`\n"
        "- `/ranking`\n"
        "- `/income [member:...]`\n"
        "- `/perks [member:...]`\n"
        "- `/nextreset`"
    ),
    "lookup": (
        "## Lookup Commands\n"
        "- `/list target:commodities`\n"
        "- `/desc [target:...]`"
    ),
    "system": (
        "## System / Utility\n"
        "- `/register`\n"
        "- `/activity` (launch embedded activity)\n"
        "- `/help`\n"
        "- `/adminhelp` (admin only)"
    ),
    "admin": (
        "## Admin Commands\n"
        "- `/addcompany`\n"
        "- `/addcommodity`\n"
        "- `/promote member:... rank:...`\n"
        "- `/admingive`\n"
        "- `/adminshow`\n"
        "- `/previewclose`\n"
        "- `/resettradinglimit`\n"
        "- `/wipe`\n"
        "- `/webadmin`\n"
        "- `/purgeall`"
    ),
    "fun": (
        "## Fun Commands\n"
        "- `/hello`\n"
        "- `/no`\n"
        "- `/laugh target:...`"
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
                    "Help panel for current commands.\n"
                    "Choose a section below."
                )
            )
        )
        if hasattr(ui, "Separator"):
            container.add_item(ui.Separator())
        container.add_item(ui.TextDisplay(content=HELP_SECTIONS[self._section]))

        top_row = ui.ActionRow()
        started_btn = discord.ui.Button(label="Get Started", style=ButtonStyle.success)
        tutorial_btn = discord.ui.Button(label="Gameplay Loop", style=ButtonStyle.primary)

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
        alerts_btn = discord.ui.Button(label="Alerts", style=ButtonStyle.secondary)
        system_btn = discord.ui.Button(label="System", style=ButtonStyle.secondary)
        lookup_btn = discord.ui.Button(label="Lookup", style=ButtonStyle.secondary)
        fun_btn = discord.ui.Button(label="Fun", style=ButtonStyle.secondary)

        async def _switch(interaction: Interaction, section: str) -> None:
            self._section = section
            self._render()
            await interaction.response.edit_message(view=self)

        market_btn.callback = lambda i: _switch(i, "market")
        economy_btn.callback = lambda i: _switch(i, "economy")
        profile_btn.callback = lambda i: _switch(i, "profile")
        alerts_btn.callback = lambda i: _switch(i, "alerts")
        system_btn.callback = lambda i: _switch(i, "system")
        lookup_btn.callback = lambda i: _switch(i, "lookup")
        fun_btn.callback = lambda i: _switch(i, "fun")

        row_2.add_item(market_btn)
        row_2.add_item(economy_btn)
        row_2.add_item(profile_btn)
        row_2.add_item(alerts_btn)
        row_2.add_item(system_btn)
        container.add_item(row_2)

        row_3 = ui.ActionRow()
        row_3.add_item(lookup_btn)
        row_3.add_item(fun_btn)
        container.add_item(row_3)

        self.add_item(container)


def setup_help(tree: app_commands.CommandTree) -> None:
    @tree.command(name="help", description="Show help panel for available commands.")
    async def help_command(interaction: Interaction) -> None:
        if not _supports_components_v2():
            await interaction.response.send_message(
                "Help requires Components V2 support in this runtime.",
                ephemeral=False,
            )
            return

        view = HelpV2View(section="get_started")
        await interaction.response.send_message(view=view, ephemeral=False)

    @tree.command(name="adminhelp", description="Show admin command help panel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_help_command(interaction: Interaction) -> None:
        if not _supports_components_v2():
            await interaction.response.send_message(
                "Help requires Components V2 support in this runtime.",
                ephemeral=True,
            )
            return

        view = HelpV2View(section="admin")
        await interaction.response.send_message(view=view, ephemeral=True)
