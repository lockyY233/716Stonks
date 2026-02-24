from __future__ import annotations

from discord import Interaction, app_commands

from stockbot.db import (
    clear_stock_alerts_for_user,
    create_stock_alert,
    delete_stock_alert,
    get_companies,
    get_stock_alerts_for_user,
    get_user,
)


def _fmt_alert_row(row: dict) -> str:
    alert_id = int(row.get("id", 0))
    symbol = str(row.get("symbol", "")).upper()
    cond = str(row.get("condition", "above")).lower()
    target = float(row.get("target_price", 0.0))
    repeat_enabled = int(row.get("repeat_enabled", 0) or 0) > 0
    mode = "repeat" if repeat_enabled else "once"
    return f"`#{alert_id}` {symbol} {cond} `${target:.2f}` · {mode}"


def setup_notify(tree: app_commands.CommandTree) -> None:
    group = app_commands.Group(
        name="notify",
        description="Manage stock price alerts.",
    )

    @group.command(name="add", description="Notify when a stock goes above/below a target price.")
    @app_commands.describe(
        symbol="Company symbol (e.g. SBC)",
        condition="Trigger condition",
        target_price="Target price in dollars",
        repeat="If true, re-triggers on future crossings. If false, one-time.",
    )
    @app_commands.choices(
        condition=[
            app_commands.Choice(name="above", value="above"),
            app_commands.Choice(name="below", value="below"),
        ]
    )
    async def notify_add(
        interaction: Interaction,
        symbol: str,
        condition: app_commands.Choice[str],
        target_price: float,
        repeat: bool = False,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        if target_price <= 0:
            await interaction.response.send_message("`target_price` must be > 0.", ephemeral=True)
            return
        user = get_user(interaction.guild.id, interaction.user.id)
        if user is None:
            await interaction.response.send_message("Please register first with `/register`.", ephemeral=True)
            return
        companies = get_companies(interaction.guild.id)
        valid_symbols = {str(c.get("symbol", "")).upper() for c in companies}
        sym = str(symbol).upper().strip()
        if sym not in valid_symbols:
            await interaction.response.send_message(f"Unknown symbol `{sym}`.", ephemeral=True)
            return
        alert_id = create_stock_alert(
            interaction.guild.id,
            interaction.user.id,
            sym,
            condition.value,
            float(target_price),
            repeat_enabled=repeat,
        )
        mode = "repeat" if repeat else "once"
        await interaction.response.send_message(
            f"Alert created: `#{alert_id}` {sym} {condition.value} `${target_price:.2f}` · {mode}.",
            ephemeral=True,
        )

    @notify_add.autocomplete("symbol")
    async def notify_add_symbol_autocomplete(
        interaction: Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        if interaction.guild is None:
            return []
        needle = current.strip().lower()
        choices: list[app_commands.Choice[str]] = []
        for c in get_companies(interaction.guild.id):
            symbol = str(c.get("symbol", "")).upper()
            name = str(c.get("name", ""))
            if not symbol:
                continue
            hay = f"{symbol} {name}".lower()
            if needle and needle not in hay:
                continue
            label = f"{symbol} · {name}"[:100]
            choices.append(app_commands.Choice(name=label, value=symbol))
            if len(choices) >= 25:
                break
        return choices

    @group.command(name="list", description="List your active stock alerts.")
    async def notify_list(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        rows = get_stock_alerts_for_user(interaction.guild.id, interaction.user.id, include_disabled=False)
        if not rows:
            await interaction.response.send_message("You have no active alerts.", ephemeral=True)
            return
        lines = ["Your active alerts:"]
        lines.extend(_fmt_alert_row(r) for r in rows[:30])
        if len(rows) > 30:
            lines.append(f"... and {len(rows) - 30} more")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @group.command(name="remove", description="Remove one alert by id.")
    @app_commands.describe(alert_id="Alert id from /notify list (e.g. 12)")
    async def notify_remove(interaction: Interaction, alert_id: int) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        ok = delete_stock_alert(interaction.guild.id, interaction.user.id, int(alert_id))
        if not ok:
            await interaction.response.send_message(f"Alert `#{alert_id}` not found.", ephemeral=True)
            return
        await interaction.response.send_message(f"Removed alert `#{alert_id}`.", ephemeral=True)

    @group.command(name="clear", description="Remove all your stock alerts.")
    async def notify_clear(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        removed = clear_stock_alerts_for_user(interaction.guild.id, interaction.user.id)
        await interaction.response.send_message(f"Removed {removed} alert(s).", ephemeral=True)

    tree.add_command(group)

