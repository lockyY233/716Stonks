from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.core.commodity_rarity import rarity_color
from stockbot.db import get_state_value, set_state_value
from stockbot.services.shop_state import get_shop_items
from stockbot.services.trading import perform_buy_commodity

def _build_store(guild_id: int) -> list[dict]:
    _bucket, rows, _available = get_shop_items(guild_id)
    return rows


class ShopPager(View):
    def __init__(self, owner_id: int, guild_id: int, rows: list[dict]) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._guild_id = guild_id
        self._rows = rows
        self._index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        self.prev.disabled = self._index <= 0
        self.next.disabled = self._index >= len(self._rows) - 1
        self.buy.disabled = not bool(self.current_row().get("in_stock", False))

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

    def build_embed(self) -> Embed:
        row = self.current_row()
        name = str(row.get("name", "Unknown"))
        price = float(row.get("price", 0.0))
        rarity = str(row.get("rarity", "common"))
        image_url = str(row.get("image_url", "") or "")
        description = str(row.get("description", "") or "No description.")
        in_stock = bool(row.get("in_stock", False))

        embed = Embed(
            title=f"Shop: {name}",
            description=description,
            color=(rarity_color(rarity) if in_stock else 0xE74C3C),
        )
        embed.add_field(name="Price", value=f"${price:.2f}", inline=True)
        embed.add_field(name="Rarity", value=rarity.title(), inline=True)
        embed.add_field(
            name="Availability",
            value="IN STOCK" if in_stock else "OUT OF STOCK",
            inline=True,
        )
        embed.set_footer(text=f"Item {self._index + 1}/{len(self._rows)}")
        if image_url:
            embed.set_image(url=image_url)
        return embed

    @button(label="Prev", style=ButtonStyle.secondary)
    async def prev(self, interaction: Interaction, _button: Button) -> None:
        self._index = max(0, self._index - 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @button(label="Next", style=ButtonStyle.secondary)
    async def next(self, interaction: Interaction, _button: Button) -> None:
        self._index = min(len(self._rows) - 1, self._index + 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @button(label="Buy", style=ButtonStyle.green)
    async def buy(self, interaction: Interaction, _button: Button) -> None:
        commodity = self.current_row()
        if not bool(commodity.get("in_stock", False)):
            await interaction.response.send_message(
                "This item is OUT OF STOCK right now.",
                ephemeral=True,
            )
            return

        stock_key = str(commodity.get("_stock_key", ""))
        if stock_key:
            sold_raw = get_state_value(stock_key)
            sold = int(sold_raw) if sold_raw and sold_raw.isdigit() else 0
            if sold >= 1:
                commodity["in_stock"] = False
                self._sync_buttons()
                await interaction.response.edit_message(embed=self.build_embed(), view=self)
                return

        _ok, message = await perform_buy_commodity(
            interaction,
            str(commodity.get("name", "")),
            1,
        )
        if message == REGISTER_REQUIRED_MESSAGE:
            await interaction.response.send_message(
                message,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        if _ok and stock_key:
            set_state_value(stock_key, "1")
            commodity["in_stock"] = False
        self._sync_buttons()

        # Always refresh the shop card first so stock state/button stays in sync.
        await interaction.response.edit_message(embed=self.build_embed(), view=self)
        await interaction.followup.send(message, ephemeral=True)


def setup_shop(tree: app_commands.CommandTree) -> None:
    @tree.command(name="shop", description="Browse the rotating commodity shop.")
    async def shop(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        rows = _build_store(interaction.guild.id)
        if not rows:
            await interaction.response.send_message(
                "No commodities available yet.",
                ephemeral=True,
            )
            return

        view = ShopPager(interaction.user.id, interaction.guild.id, rows)
        await interaction.response.send_message(
            embed=view.build_embed(),
            view=view,
        )
