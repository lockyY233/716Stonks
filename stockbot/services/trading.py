from datetime import datetime, timezone

from discord import Interaction

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE
from stockbot.db import (
    get_company,
    get_user,
    get_user_shares,
    set_holding,
    update_company_trade_volume,
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
        return False, REGISTER_REQUIRED_MESSAGE

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
    update_company_trade_volume(
        interaction.guild.id,
        symbol.upper(),
        float(shares),
        0.0,
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
        return False, REGISTER_REQUIRED_MESSAGE

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
    update_company_trade_volume(
        interaction.guild.id,
        symbol.upper(),
        0.0,
        float(shares),
        datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Sold {shares} shares of {symbol.upper()} for ${total_gain}."
