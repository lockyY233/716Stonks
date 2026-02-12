from random import Random

from discord import ButtonStyle, Embed, Interaction, app_commands
from discord.ui import Button, View, button

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config import TRADING_LIMITS_PERIOD
from stockbot.core.commodity_rarity import rarity_color
from stockbot.db import get_commodities, get_state_value
from stockbot.services.trading import perform_buy_commodity


def _current_limit_bucket() -> int:
    period = max(1, int(TRADING_LIMITS_PERIOD))
    raw = get_state_value("last_tick")
    tick = int(raw) if raw is not None else 0
    return max(0, tick // period)


def _build_store(guild_id: int) -> list[dict]:
    commodities = get_commodities(guild_id)
    if not commodities:
        return []

    bucket = _current_limit_bucket()
    rng = Random(f"shop:{guild_id}:{bucket}")
    indices = list(range(len(commodities)))
    rng.shuffle(indices)
    selected = [commodities[i] for i in indices[:5]]

    store_rows: list[dict] = []
    for row in selected:
        entry = dict(row)
        entry["in_stock"] = rng.random() < 0.75
        store_rows.append(entry)
    return store_rows


class ShopPager(View):
    def __init__(self, owner_id: int, rows: list[dict]) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
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
        await interaction.response.send_message(message, ephemeral=True)


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

        view = ShopPager(interaction.user.id, rows)
        await interaction.response.send_message(
            embed=view.build_embed(),
            view=view,
        )
