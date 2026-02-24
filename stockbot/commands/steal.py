from __future__ import annotations

import random
import time
from datetime import datetime, timezone

from discord import Interaction, Member, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import add_action_history, get_state_value, get_user, set_state_value, update_user_bank
from stockbot.services.money import money
from stockbot.services.perks import evaluate_user_perks

STEAL_COOLDOWN_SECONDS = 1800
BASE_STEAL_CHANCE = 0.25
BASE_STEAL_MIN = 50.0
BASE_STEAL_MAX = 100.0
PITY_EVENT_CHANCE = 0.01
LOW_BALANCE_PITY_THRESHOLD = 200.0
LOW_BALANCE_PITY_MIN = 1.0
LOW_BALANCE_PITY_MAX = 100.0


def _cooldown_key(guild_id: int, user_id: int) -> str:
    return f"steal_cooldown:{int(guild_id)}:{int(user_id)}"


def _fmt_remaining(seconds: int) -> str:
    sec = max(0, int(seconds))
    mm, ss = divmod(sec, 60)
    hh, mm = divmod(mm, 60)
    if hh > 0:
        return f"{hh}h {mm}m {ss}s"
    return f"{mm}m {ss}s"


def setup_steal(tree: app_commands.CommandTree) -> None:
    @tree.command(name="steal", description="Try to steal cash from a richer player (1 attempt per hour).")
    @app_commands.describe(member="The target player (must be richer than you).")
    async def steal(interaction: Interaction, member: Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.", ephemeral=True)
            return
        if member.id == interaction.user.id:
            await interaction.response.send_message("You cannot steal from yourself.", ephemeral=True)
            return
        if member.bot:
            await interaction.response.send_message("You cannot steal from bots.", ephemeral=True)
            return

        thief = get_user(interaction.guild.id, interaction.user.id)
        if thief is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
                ephemeral=True,
            )
            return
        victim = get_user(interaction.guild.id, member.id)
        if victim is None:
            await interaction.response.send_message(f"{member.mention} is not registered yet.", ephemeral=True)
            return

        thief_bank = float(thief.get("bank", 0.0) or 0.0)
        victim_bank = float(victim.get("bank", 0.0) or 0.0)

        now = int(time.time())
        key = _cooldown_key(interaction.guild.id, interaction.user.id)
        raw_next = get_state_value(key)
        try:
            next_epoch = int(str(raw_next or "0"))
        except (TypeError, ValueError):
            next_epoch = 0
        if next_epoch > now:
            await interaction.response.send_message(
                f"You can steal again in {_fmt_remaining(next_epoch - now)}.",
                ephemeral=True,
            )
            return

        perk_eval = evaluate_user_perks(
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            base_income=0.0,
            base_steal_chance=BASE_STEAL_CHANCE,
            base_steal_amount_min=BASE_STEAL_MIN,
            base_steal_amount_max=BASE_STEAL_MAX,
        )
        chance = max(0.0, min(1.0, float(perk_eval["final"]["steal_chance"])))
        amount_min = max(0.01, float(perk_eval["final"]["steal_amount_min"]))
        amount_max = max(0.01, float(perk_eval["final"]["steal_amount_max"]))
        if amount_min > amount_max:
            amount_min, amount_max = amount_max, amount_min

        # Attempt consumes cooldown regardless of success.
        set_state_value(key, str(now + STEAL_COOLDOWN_SECONDS))

        # Compassion branch for low-balance targets.
        if victim_bank < LOW_BALANCE_PITY_THRESHOLD:
            pity_amount = money(random.uniform(LOW_BALANCE_PITY_MIN, LOW_BALANCE_PITY_MAX))
            pity_transfer = money(min(thief_bank, pity_amount))
            if pity_transfer <= 0:
                await interaction.response.send_message(
                    "You wanted to help, but you don't have enough money to give right now.",
                    ephemeral=True,
                )
                return
            update_user_bank(interaction.guild.id, interaction.user.id, money(thief_bank - pity_transfer))
            update_user_bank(interaction.guild.id, member.id, money(victim_bank + pity_transfer))
            now_iso = datetime.now(timezone.utc).isoformat()
            add_action_history(
                guild_id=interaction.guild.id,
                user_id=interaction.user.id,
                action_type="steal_pity_give",
                target_type="user",
                target_symbol=str(member.id),
                quantity=1.0,
                unit_price=float(pity_transfer),
                total_amount=float(-pity_transfer),
                details=f"target={member.id};reason=target_bank_below_{LOW_BALANCE_PITY_THRESHOLD:.0f}",
                created_at=now_iso,
            )
            add_action_history(
                guild_id=interaction.guild.id,
                user_id=member.id,
                action_type="steal_pity_receive",
                target_type="user",
                target_symbol=str(interaction.user.id),
                quantity=1.0,
                unit_price=float(pity_transfer),
                total_amount=float(pity_transfer),
                details=f"source={interaction.user.id};reason=target_bank_below_{LOW_BALANCE_PITY_THRESHOLD:.0f}",
                created_at=now_iso,
            )
            await interaction.response.send_message(
                (
                    "You stole his wallet but there is no money in there. "
                    f"You felt so bad and give him: ${pity_transfer:.2f}. "
                    "Hopefully his life will be better."
                ),
                ephemeral=False,
            )
            return

        # Rare tragedy branch (1%): donate instead of stealing.
        tragedy_roll = random.random()
        if tragedy_roll < PITY_EVENT_CHANCE:
            pity_amount = money(random.uniform(amount_min, amount_max))
            pity_transfer = money(min(thief_bank, pity_amount))
            if pity_transfer <= 0:
                await interaction.response.send_message(
                    "You felt bad but couldn't give anything because your balance is too low.",
                    ephemeral=True,
                )
                return
            update_user_bank(interaction.guild.id, interaction.user.id, money(thief_bank - pity_transfer))
            update_user_bank(interaction.guild.id, member.id, money(victim_bank + pity_transfer))
            now_iso = datetime.now(timezone.utc).isoformat()
            add_action_history(
                guild_id=interaction.guild.id,
                user_id=interaction.user.id,
                action_type="steal_pity_give",
                target_type="user",
                target_symbol=str(member.id),
                quantity=1.0,
                unit_price=float(pity_transfer),
                total_amount=float(-pity_transfer),
                details=(
                    f"target={member.id};reason=tragedy_roll;roll={tragedy_roll:.4f};"
                    f"range={amount_min:.2f}-{amount_max:.2f}"
                ),
                created_at=now_iso,
            )
            add_action_history(
                guild_id=interaction.guild.id,
                user_id=member.id,
                action_type="steal_pity_receive",
                target_type="user",
                target_symbol=str(interaction.user.id),
                quantity=1.0,
                unit_price=float(pity_transfer),
                total_amount=float(pity_transfer),
                details=(
                    f"source={interaction.user.id};reason=tragedy_roll;roll={tragedy_roll:.4f};"
                    f"range={amount_min:.2f}-{amount_max:.2f}"
                ),
                created_at=now_iso,
            )
            await interaction.response.send_message(
                (
                    f"You were trying to steal {member.mention} money, but {member.mention} has started crying about his tragedy past. "
                    f"You felt so bad and gifted him: ${pity_transfer:.2f}"
                ),
                ephemeral=False,
            )
            return

        if victim_bank <= thief_bank:
            await interaction.response.send_message(
                f"You can only steal from someone richer than you. Your bank: ${thief_bank:.2f}, target bank: ${victim_bank:.2f}.",
                ephemeral=True,
            )
            return

        roll = random.random()
        success = roll < chance
        if not success:
            await interaction.response.send_message(
                f"{interaction.user.mention} tried to steal from {member.mention} and failed.",
                ephemeral=False,
            )
            return

        rolled = money(random.uniform(amount_min, amount_max))
        transfer = money(min(victim_bank, rolled))
        if transfer <= 0:
            await interaction.response.send_message(
                f"{interaction.user.mention} found no cash to steal from {member.mention}.",
                ephemeral=False,
            )
            return

        update_user_bank(interaction.guild.id, interaction.user.id, money(thief_bank + transfer))
        update_user_bank(interaction.guild.id, member.id, money(victim_bank - transfer))

        now_iso = datetime.now(timezone.utc).isoformat()
        add_action_history(
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            action_type="steal",
            target_type="user",
            target_symbol=str(member.id),
            quantity=1.0,
            unit_price=float(transfer),
            total_amount=float(transfer),
            details=(
                f"target={member.id};chance={chance:.4f};roll={roll:.4f};"
                f"range={amount_min:.2f}-{amount_max:.2f};success=1"
            ),
            created_at=now_iso,
        )
        add_action_history(
            guild_id=interaction.guild.id,
            user_id=member.id,
            action_type="stolen",
            target_type="user",
            target_symbol=str(interaction.user.id),
            quantity=1.0,
            unit_price=float(transfer),
            total_amount=float(-transfer),
            details=(
                f"source={interaction.user.id};chance={chance:.4f};roll={roll:.4f};"
                f"range={amount_min:.2f}-{amount_max:.2f};success=1"
            ),
            created_at=now_iso,
        )

        await interaction.response.send_message(
            f"{interaction.user.mention} successfully stole **${transfer:.2f}** from {member.mention}.",
            ephemeral=False,
        )
