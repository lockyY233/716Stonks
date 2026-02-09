from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import View, button


class HelpView(View):
    def __init__(self, owner_id: int) -> None:
        super().__init__(timeout=180)
        self._owner_id = owner_id

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can use these buttons.",
                ephemeral=True,
            )
            return False
        return True

    async def _send_detail(self, interaction: Interaction, title: str, usage: str, detail: str) -> None:
        embed = Embed(title=title, description=detail)
        embed.add_field(name="Usage", value=f"`{usage}`", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @button(label="/register", style=ButtonStyle.secondary)
    async def register(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/register",
            usage="/register",
            detail="Register your account.",
        )

    @button(label="/buy", style=ButtonStyle.secondary)
    async def buy(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/buy",
            usage="/buy",
            detail="Buy shares of a company (opens an interactive flow).",
        )

    @button(label="/sell", style=ButtonStyle.secondary)
    async def sell(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/sell",
            usage="/sell",
            detail="Sell shares of a company (opens an interactive flow).",
        )

    @button(label="/price", style=ButtonStyle.secondary)
    async def price(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/price",
            usage="/price symbol:AAPL last:50",
            detail="Show a price chart for a company.",
        )

    @button(label="/status", style=ButtonStyle.secondary)
    async def status(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/status",
            usage="/status member:@user",
            detail="Show your status and holdings.",
        )

    @button(label="/list", style=ButtonStyle.secondary)
    async def list_cmd(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/list",
            usage="/list target:companies or /list target:players",
            detail="List available companies or registered players (paged).",
        )

    @button(label="/hello", style=ButtonStyle.secondary)
    async def hello(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/hello",
            usage="/hello",
            detail="Say hello to the bot (random response).",
        )

    @button(label="/no", style=ButtonStyle.secondary)
    async def no(self, interaction: Interaction, _button) -> None:
        await self._send_detail(
            interaction,
            title="/no",
            usage="/no member:@user",
            detail="Disagree with someone.",
        )


def setup_help(tree: app_commands.CommandTree) -> None:
    @tree.command(name="help", description="List available commands.")
    async def help_command(interaction: Interaction) -> None:
        embed = Embed(
            title="Commands",
            description="Tap a button for details.",
        )
        view = HelpView(interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
