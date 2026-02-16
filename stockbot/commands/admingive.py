from datetime import datetime, timezone

from discord import Interaction, Member, app_commands

from stockbot.config.runtime import get_app_config
from stockbot.db import (
    add_action_history,
    get_commodities,
    get_commodity,
    get_user,
    get_user_commodities,
    recalc_user_networth,
    upsert_user_commodity,
)
from stockbot.services.perks import check_and_announce_perk_activations


def setup_admingive(tree: app_commands.CommandTree) -> None:
    @tree.command(name="admingive", description="Admin: give commodity items to a player.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        target="Target player.",
        commodity="Commodity name to give.",
        quantity="How many units to give (default: 1).",
    )
    async def admingive(
        interaction: Interaction,
        target: Member,
        commodity: str,
        quantity: app_commands.Range[int, 1, 1000] = 1,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        user = get_user(interaction.guild.id, target.id)
        if user is None:
            await interaction.response.send_message(
                f"{target.mention} is not registered.",
                ephemeral=True,
            )
            return

        row = get_commodity(interaction.guild.id, commodity.strip())
        if row is None:
            await interaction.response.send_message(
                f"Commodity `{commodity}` not found.",
                ephemeral=True,
            )
            return

        limit = int(get_app_config("COMMODITIES_LIMIT"))
        holdings = get_user_commodities(interaction.guild.id, target.id)
        owned_total = sum(max(0, int(item.get("quantity", 0))) for item in holdings)
        if limit > 0:
            if owned_total >= limit:
                await interaction.response.send_message(
                    (
                        f"{target.mention} is already at the commodity cap "
                        f"({owned_total}/{limit})."
                    ),
                    ephemeral=True,
                )
                return
            if owned_total + quantity > limit:
                remaining = max(0, limit - owned_total)
                await interaction.response.send_message(
                    (
                        f"Cannot give {quantity}. {target.mention} can only receive "
                        f"{remaining} more item(s) before hitting the cap ({limit})."
                    ),
                    ephemeral=True,
                )
                return

        commodity_name = str(row.get("name", commodity.strip()))
        upsert_user_commodity(
            interaction.guild.id,
            target.id,
            commodity_name,
            int(quantity),
        )
        recalc_user_networth(interaction.guild.id, target.id)
        add_action_history(
            guild_id=interaction.guild.id,
            user_id=target.id,
            action_type="admingive",
            target_type="commodity",
            target_symbol=commodity_name,
            quantity=float(quantity),
            unit_price=float(row.get("price", 0.0)),
            total_amount=0.0,
            details=f"Granted by admin {interaction.user.id}",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        activated = await check_and_announce_perk_activations(interaction, target_user_id=target.id)

        new_total = owned_total + int(quantity)
        cap_text = (
            f" ({new_total}/{limit} total items)"
            if limit > 0
            else f" ({new_total} total items)"
        )
        perk_text = ""
        if activated:
            perk_text = "\nActivated perks: " + ", ".join(f"`{name}`" for name in activated)
        await interaction.response.send_message(
            f"Gave {quantity}x **{commodity_name}** to {target.mention}.{cap_text}{perk_text}",
            ephemeral=True,
        )

    @admingive.autocomplete("commodity")
    async def admingive_commodity_autocomplete(
        interaction: Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        if interaction.guild is None:
            return []
        query = current.strip().lower()
        rows = get_commodities(interaction.guild.id)
        names = [str(row.get("name", "")) for row in rows if str(row.get("name", ""))]
        if query:
            names = [name for name in names if query in name.lower()]
        return [app_commands.Choice(name=name, value=name) for name in names[:25]]
