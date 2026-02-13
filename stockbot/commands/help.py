from discord import ButtonStyle, Embed, Interaction, Member, app_commands
from discord.ui import View, button


class HelpView(View):
    def __init__(self, owner_id: int, show_admin: bool) -> None:
        super().__init__(timeout=180)
        self._owner_id = owner_id
        if not show_admin:
            self.remove_item(self.addcompany)
            self.remove_item(self.addcommodity)
            self.remove_item(self.adminshow)
            self.remove_item(self.webadmin)

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can use these buttons.",
                ephemeral=False,
            )
            return False
        return True

    async def _send_detail(self, interaction: Interaction, title: str, usage: str, detail: str) -> None:
        embed = Embed(title=title, description=detail)
        embed.add_field(name="Usage", value=f"`{usage}`", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @button(label="Get Started", style=ButtonStyle.success, row=0)
    async def get_started(self, interaction: Interaction, _button) -> None:
        embed = Embed(
            title="Get Started",
            description=(
                "Quick start guide for new players:\n\n"
                "1. Use `/register` to create your account.\n"
                "2. Use `/list target:companies` to see available stocks.\n"
                "3. Use `/price symbol:...` to check charts and current price.\n"
                "4. Buy/Sell stocks with `/buy` and `/sell` (or the Buy button in `/price`).\n"
                "5. Use `/shop` to buy commodities and build networth.\n"
                "6. Higher army ranks receive different income at market close.\n\n"
                "Important rules:\n"
                "- Trading actions are limited per reset window.\n"
                "- Selling charges a fee on realized profit."
            ),
        )
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @button(label="/register", style=ButtonStyle.secondary, row=0)
    async def register(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/register", "/register", "Register your account.")

    @button(label="/buy", style=ButtonStyle.secondary, row=0)
    async def buy(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/buy", "/buy", "Buy shares or commodities (interactive flow).")

    @button(label="/sell", style=ButtonStyle.secondary, row=0)
    async def sell(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/sell", "/sell", "Sell shares (interactive flow).")

    @button(label="/price", style=ButtonStyle.secondary, row=0)
    async def price(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/price", "/price symbol:ATEST last:50", "Show company price chart.")

    @button(label="/status", style=ButtonStyle.secondary, row=1)
    async def status(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/status", "/status member:@user", "Show profile and holdings.")

    @button(label="/shop", style=ButtonStyle.secondary, row=1)
    async def shop(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/shop", "/shop", "Browse rotating commodity shop.")

    @button(label="/bank", style=ButtonStyle.secondary, row=1)
    async def bank(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/bank", "/bank", "Open bank services (loan/payback).")

    @button(label="/nextreset", style=ButtonStyle.secondary, row=1)
    async def nextreset(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/nextreset", "/nextreset", "Show minutes until trading-limit reset.")

    @button(label="/feedback", style=ButtonStyle.secondary, row=1)
    async def feedback(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/feedback", "/feedback", "Send anonymous feedback.")

    @button(label="/list", style=ButtonStyle.secondary, row=2)
    async def list_cmd(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/list", "/list target:companies|commodities|players", "Show paged lists.")

    @button(label="/desc", style=ButtonStyle.secondary, row=2)
    async def desc(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/desc", "/desc target:company_or_commodity", "Show lore/description.")

    @button(label="/perks", style=ButtonStyle.secondary, row=2)
    async def perks(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/perks", "/perks member:@user", "Show triggered perks and projected daily income.")

    @button(label="/addcompany", style=ButtonStyle.primary, row=3)
    async def addcompany(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/addcompany", "/addcompany ...", "Admin: add a company.")

    @button(label="/addcommodity", style=ButtonStyle.primary, row=3)
    async def addcommodity(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/addcommodity", "/addcommodity ...", "Admin: add a commodity.")

    @button(label="/adminshow", style=ButtonStyle.primary, row=3)
    async def adminshow(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/adminshow", "/adminshow target:users|companies|commodities", "Admin: inspect/edit rows.")

    @button(label="/webadmin", style=ButtonStyle.primary, row=3)
    async def webadmin(self, interaction: Interaction, _button) -> None:
        await self._send_detail(interaction, "/webadmin", "/webadmin", "Admin: generate dashboard token link.")


def setup_help(tree: app_commands.CommandTree) -> None:
    @tree.command(name="help", description="List available commands.")
    async def help_command(interaction: Interaction) -> None:
        show_admin = False
        if isinstance(interaction.user, Member):
            show_admin = bool(interaction.user.guild_permissions.administrator)
        elif interaction.guild is not None:
            member = interaction.guild.get_member(interaction.user.id)
            if member is not None:
                show_admin = bool(member.guild_permissions.administrator)

        embed = Embed(
            title="Commands",
            description="Tap a button for details.",
        )
        if show_admin:
            embed.add_field(name="Mode", value="Admin view enabled.", inline=False)
        else:
            embed.add_field(name="Mode", value="Player view.", inline=False)

        view = HelpView(interaction.user.id, show_admin=show_admin)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=False)
