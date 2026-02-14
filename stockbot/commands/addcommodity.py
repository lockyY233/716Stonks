from discord import Embed, Interaction, app_commands

from stockbot.core.commodity_rarity import normalize_rarity, rarity_color
from stockbot.db import add_commodity


def setup_addcommodity(tree: app_commands.CommandTree) -> None:
    @tree.command(name="addcommodity", description="Admin: add a commodity.")
    @app_commands.describe(
        name="Commodity name (e.g. Gold).",
        price="Commodity unit price.",
        rarity="Rarity tier.",
        spawn_weight_override="Optional per-item shop spawn weight override (0 = use rarity weight).",
        image_url="Image URL for this commodity.",
        description="Commodity description/lore.",
    )
    @app_commands.choices(
        rarity=[
            app_commands.Choice(name="common", value="common"),
            app_commands.Choice(name="uncommon", value="uncommon"),
            app_commands.Choice(name="rare", value="rare"),
            app_commands.Choice(name="legendary", value="legendary"),
            app_commands.Choice(name="exotic", value="exotic"),
        ]
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def addcommodity(
        interaction: Interaction,
        name: str,
        price: float,
        rarity: str,
        image_url: str,
        description: str,
        spawn_weight_override: float = 0.0,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        created = add_commodity(
            guild_id=interaction.guild.id,
            name=name.strip(),
            price=price,
            rarity=normalize_rarity(rarity),
            spawn_weight_override=max(0.0, float(spawn_weight_override)),
            image_url=image_url.strip(),
            description=description.strip(),
        )
        if not created:
            await interaction.response.send_message(
                f"Commodity `{name}` already exists.",
                ephemeral=True,
            )
            return

        embed = Embed(
            title=f"Commodity `{name}` added",
            description=description.strip() or "No description.",
            color=rarity_color(rarity),
        )
        embed.add_field(name="Price", value=f"${float(price):.2f}", inline=True)
        embed.add_field(name="Rarity", value=normalize_rarity(rarity).title(), inline=True)
        embed.add_field(
            name="Shop Weight",
            value=f"{max(0.0, float(spawn_weight_override)):.4g} (0 = rarity default)",
            inline=True,
        )
        if image_url.strip():
            embed.set_image(url=image_url.strip())
        await interaction.response.send_message(embed=embed, ephemeral=True)
