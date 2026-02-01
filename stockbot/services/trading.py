from datetime import datetime, timezone

from discord import Interaction

from stockbot.db import (
    get_company,
    get_user,
    get_user_shares,
    set_holding,
    update_company_slope,
    update_user_bank,
    upsert_holding,
)


async def perform_buy(
    interaction: Interaction,
    symbol: str,
    shares: int,
) -> tuple[bool, str]:
    if interaction.guild is None:
        return False, "Please use this command in a server."

    if shares <= 0:
        return False, "Shares must be a positive integer."

    user = get_user(interaction.guild.id, interaction.user.id)
    if user is None:
        return False, "You are not registered yet. Use /register first."

    company = get_company(interaction.guild.id, symbol.upper())
    if company is None:
        return False, f"Company `{symbol.upper()}` not found."

    price = float(company.get("current_price", company["base_price"]))
    total_cost = round(price * shares, 2)
    if user["bank"] < total_cost:
        return False, f"Not enough funds. Need ${total_cost}, you have ${user['bank']}."

    new_bank = float(user["bank"]) - total_cost
    update_user_bank(interaction.guild.id, interaction.user.id, new_bank)
    upsert_holding(interaction.guild.id, interaction.user.id, symbol.upper(), shares)
    slope = float(company.get("slope", 0))
    player_impact = float(company.get("player_impact", 0.5))
    update_company_slope(
        interaction.guild.id,
        symbol.upper(),
        slope + (shares * player_impact),
        datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Bought {shares} shares of {symbol.upper()} for ${total_cost}."


async def perform_sell(
    interaction: Interaction,
    symbol: str,
    shares: int,
) -> tuple[bool, str]:
    if interaction.guild is None:
        return False, "Please use this command in a server."

    if shares <= 0:
        return False, "Shares must be a positive integer."

    user = get_user(interaction.guild.id, interaction.user.id)
    if user is None:
        return False, "You are not registered yet. Use /register first."

    company = get_company(interaction.guild.id, symbol.upper())
    if company is None:
        return False, f"Company `{symbol.upper()}` not found."

    owned = get_user_shares(interaction.guild.id, interaction.user.id, symbol.upper())
    if owned < shares:
        return False, f"You only own {owned} shares of {symbol.upper()}."

    price = float(company.get("current_price", company["base_price"]))
    total_gain = round(price * shares, 2)
    new_bank = float(user["bank"]) + total_gain
    update_user_bank(interaction.guild.id, interaction.user.id, new_bank)
    set_holding(interaction.guild.id, interaction.user.id, symbol.upper(), owned - shares)
    slope = float(company.get("slope", 0))
    player_impact = float(company.get("player_impact", 0.5))
    update_company_slope(
        interaction.guild.id,
        symbol.upper(),
        slope - (shares * player_impact),
        datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Sold {shares} shares of {symbol.upper()} for ${total_gain}."
