from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
import time

import discord
from discord import ButtonStyle, Embed, Interaction, app_commands

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config.runtime import get_app_config
from stockbot.db import add_action_history, get_state_value, get_user, set_state_value, update_user_bank
from stockbot.services.money import money
from stockbot.services.perks import evaluate_user_perks


@dataclass(frozen=True)
class SlotSymbol:
    emoji: str
    weight: int


_SYMBOLS: tuple[SlotSymbol, ...] = (
    SlotSymbol("🍒", 28),
    SlotSymbol("🍋", 24),
    SlotSymbol("🍉", 18),
    SlotSymbol("🔔", 14),
    SlotSymbol("⭐", 10),
    SlotSymbol("7️⃣", 5),
    SlotSymbol("💎", 1),
)
_TRIPLE_CONFIG_BY_EMOJI: dict[str, str] = {
    "🍒": "SLOT_TRIPLE_MULT_CHERRY",
    "🍋": "SLOT_TRIPLE_MULT_LEMON",
    "🍉": "SLOT_TRIPLE_MULT_WATERMELON",
    "🔔": "SLOT_TRIPLE_MULT_BELL",
    "⭐": "SLOT_TRIPLE_MULT_STAR",
    "7️⃣": "SLOT_TRIPLE_MULT_SEVEN",
    "💎": "SLOT_TRIPLE_MULT_DIAMOND",
}


def _normalize_bounds(low: float, high: float) -> tuple[float, float]:
    if low <= high:
        return low, high
    return high, low


def _spin_reels() -> tuple[SlotSymbol, SlotSymbol, SlotSymbol]:
    picked = random.choices(
        _SYMBOLS,
        weights=[s.weight for s in _SYMBOLS],
        k=3,
    )
    return picked[0], picked[1], picked[2]


def _evaluate_spin(
    reels: tuple[SlotSymbol, SlotSymbol, SlotSymbol],
    *,
    effective_bet: float,
    input_multiplier: float,
    perk_win_multiplier: float,
    pair_base_payout: float,
    triple_mult_by_emoji: dict[str, float],
) -> tuple[float, str]:
    a, b, c = reels
    ems = [a.emoji, b.emoji, c.emoji]
    counts: dict[str, int] = {}
    for em in ems:
        counts[em] = counts.get(em, 0) + 1
    top = max(counts.values())

    if top == 3:
        symbol_mult = max(0.0, float(triple_mult_by_emoji.get(a.emoji, 0.0)))
        payout = effective_bet * symbol_mult * input_multiplier * perk_win_multiplier
        return money(payout), f"JACKPOT x{symbol_mult:.2f}"
    if top == 2:
        payout = effective_bet * max(0.0, pair_base_payout) * input_multiplier * perk_win_multiplier
        return money(payout), f"PAIR x{pair_base_payout:.2f}"
    return 0.0, "MISS"


def _build_embed(
    *,
    user: discord.abc.User,
    bet: float,
    spins_requested: int,
    spins_done: int,
    perk_win_multiplier: float,
    effective_bet: float,
    net_delta: float,
    new_bank: float,
    spins_used_hour: int,
    spins_limit_hour: int,
    lines: list[str],
) -> Embed:
    title = f"🎰 Slot Machine ({int(spins_used_hour)}/{int(spins_limit_hour)})"
    desc = "\n".join(lines) if lines else "No spins executed."
    color = 0x2ECC71 if net_delta >= 0 else 0xE74C3C
    embed = Embed(title=title, description=desc, color=color)
    embed.add_field(name="Base Bet / Spin", value=f"${bet:.2f}", inline=True)
    embed.add_field(name="Effective Bet / Spin", value=f"${effective_bet:.2f}", inline=True)
    embed.add_field(name="Spins", value=f"{spins_done}/{spins_requested}", inline=True)
    embed.add_field(name="Perk Win Mult", value=f"x{perk_win_multiplier:.2f}", inline=True)
    embed.add_field(name="Net", value=f"${net_delta:+.2f}", inline=True)
    embed.add_field(name="New Balance", value=f"${new_bank:.2f}", inline=True)
    embed.set_footer(text=f"Player: {user.display_name}")
    return embed


def _slot_hour_window_start(epoch_seconds: int) -> int:
    return (int(epoch_seconds) // 3600) * 3600


def _slot_hour_usage_key(guild_id: int, user_id: int, hour_start_epoch: int) -> str:
    return f"slot_hour_usage:{int(guild_id)}:{int(user_id)}:{int(hour_start_epoch)}"


async def _run_slot(
    interaction: Interaction,
    *,
    bet: float,
    spins: int,
) -> tuple[bool, str, Embed | None]:
    if interaction.guild is None:
        return False, "Please use this command in a server.", None

    user_row = get_user(interaction.guild.id, interaction.user.id)
    if user_row is None:
        return False, REGISTER_REQUIRED_MESSAGE, None

    base_bet_min = float(get_app_config("SLOT_BET_MIN"))
    base_bet_max = float(get_app_config("SLOT_BET_MAX"))
    max_spins_cfg = max(1, int(get_app_config("SLOT_MAX_SPINS")))
    input_multiplier = 1.0
    pair_base_payout = max(0.0, float(get_app_config("SLOT_PAIR_PAYOUT_BASE")))
    triple_mult_by_emoji = {
        emoji: max(0.0, float(get_app_config(cfg_key)))
        for emoji, cfg_key in _TRIPLE_CONFIG_BY_EMOJI.items()
    }
    base_bet_min, base_bet_max = _normalize_bounds(max(0.01, base_bet_min), max(0.01, base_bet_max))

    perk_eval = evaluate_user_perks(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        base_income=0.0,
        base_slot_bet_multiplier=1.0,
        base_slot_bet_limit=base_bet_max,
        base_slot_win_multiplier=1.0,
        base_slot_hourly_spin_limit=float(get_app_config("SLOT_HOURLY_SPIN_LIMIT")),
    )
    perk_bet_multiplier = max(0.01, float(perk_eval["final"]["slot_bet_multiplier"]))
    perk_bet_limit = max(base_bet_min, float(perk_eval["final"]["slot_bet_limit"]))
    perk_win_multiplier = max(0.0, float(perk_eval["final"]["slot_win_multiplier"]))
    hourly_spin_limit = max(0, int(float(perk_eval["final"]["slot_hourly_spin_limit"])))

    if bet < base_bet_min:
        return False, f"Bet is below minimum (${base_bet_min:.2f}).", None
    if bet > perk_bet_limit:
        return False, f"Bet exceeds your max (${perk_bet_limit:.2f}).", None
    if spins <= 0:
        return False, "Spins must be a positive integer.", None

    spins_cap = max(1, max_spins_cfg)
    now_epoch = int(time.time())
    hour_start = _slot_hour_window_start(now_epoch)
    usage_key = _slot_hour_usage_key(interaction.guild.id, interaction.user.id, hour_start)
    used_raw = get_state_value(usage_key)
    try:
        used_this_hour = max(0, int(str(used_raw or "0")))
    except (TypeError, ValueError):
        used_this_hour = 0
    spins_left_before = max(0, hourly_spin_limit - used_this_hour)
    if spins_left_before <= 0:
        return False, "Hourly slot spin limit reached. Try again next hour.", None

    spins_requested = min(spins, spins_cap, spins_left_before)
    effective_bet = money(bet * perk_bet_multiplier)
    if effective_bet <= 0:
        return False, "Effective bet is invalid.", None

    bank_now = float(user_row.get("bank", 0.0) or 0.0)
    if bank_now < effective_bet:
        return False, f"Not enough funds. Need ${effective_bet:.2f}, you have ${bank_now:.2f}.", None

    lines: list[str] = []
    total_wagered = 0.0
    total_payout = 0.0
    spins_done = 0
    for idx in range(spins_requested):
        if bank_now < effective_bet:
            break
        bank_now = money(bank_now - effective_bet)
        total_wagered = money(total_wagered + effective_bet)
        reels = _spin_reels()
        payout, tier = _evaluate_spin(
            reels,
            effective_bet=effective_bet,
            input_multiplier=input_multiplier,
            perk_win_multiplier=perk_win_multiplier,
            pair_base_payout=pair_base_payout,
            triple_mult_by_emoji=triple_mult_by_emoji,
        )
        if payout > 0:
            bank_now = money(bank_now + payout)
            total_payout = money(total_payout + payout)
            lines.append(f"`#{idx + 1}` {' '.join(r.emoji for r in reels)} · **{tier}** · +${payout:.2f}")
        else:
            lines.append(f"`#{idx + 1}` {' '.join(r.emoji for r in reels)} · {tier}")
        spins_done += 1

    if spins_done <= 0:
        return False, "Unable to execute a spin with current balance.", None

    update_user_bank(interaction.guild.id, interaction.user.id, bank_now)
    used_after = used_this_hour + spins_done
    set_state_value(usage_key, str(used_after))
    net_delta = money(total_payout - total_wagered)
    add_action_history(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action_type="slot",
        target_type="casino",
        target_symbol="SLOT",
        quantity=float(spins_done),
        unit_price=float(effective_bet),
        total_amount=float(net_delta),
        details=(
            f"bet={bet:.2f};effective_bet={effective_bet:.2f};spins={spins_done};"
            f"input_mult={input_multiplier:.2f};perk_bet_mult={perk_bet_multiplier:.2f};"
            f"perk_win_mult={perk_win_multiplier:.2f};wagered={total_wagered:.2f};payout={total_payout:.2f};"
            f"hourly_limit={hourly_spin_limit};hourly_used={used_after}"
        ),
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    if spins_done < spins_requested:
        lines.append(f"_Stopped early: insufficient funds for spin {spins_done + 1}._")
    if spins_requested < min(spins, spins_cap):
        lines.append("_Stopped early by hourly spin limit._")

    embed = _build_embed(
        user=interaction.user,
        bet=money(bet),
        spins_requested=spins_requested,
        spins_done=spins_done,
        perk_win_multiplier=perk_win_multiplier,
        effective_bet=effective_bet,
        net_delta=net_delta,
        new_bank=bank_now,
        spins_used_hour=used_after,
        spins_limit_hour=hourly_spin_limit,
        lines=lines,
    )
    return True, "", embed


class SlotRepeatView(discord.ui.View):
    def __init__(self, owner_id: int, *, bet: float, spins: int) -> None:
        super().__init__(timeout=300)
        self._owner_id = owner_id
        self._bet = float(bet)
        self._spins = int(spins)

    @discord.ui.button(label="Spin Again", style=ButtonStyle.primary)
    async def spin_again(self, interaction: Interaction, _button: discord.ui.Button) -> None:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message("Only the command user can use this button.", ephemeral=True)
            return
        ok, message, embed = await _run_slot(
            interaction,
            bet=self._bet,
            spins=self._spins,
        )
        if not ok:
            if message == REGISTER_REQUIRED_MESSAGE:
                await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            return
        next_view = SlotRepeatView(
            self._owner_id,
            bet=self._bet,
            spins=self._spins,
        )
        await interaction.response.send_message(embed=embed, view=next_view, ephemeral=False)


def setup_slot(tree: app_commands.CommandTree) -> None:
    @tree.command(name="slot", description="Spin a single-player slot machine.")
    @app_commands.describe(
        bet="Base bet per spin.",
        spins="How many spins to run using the same bet.",
    )
    async def slot(
        interaction: Interaction,
        bet: app_commands.Range[float, 0.01, 1000000.0] = 50.0,
        spins: app_commands.Range[int, 1, 50] = 1,
    ) -> None:
        ok, message, embed = await _run_slot(
            interaction,
            bet=float(bet),
            spins=int(spins),
        )
        if not ok:
            if message == REGISTER_REQUIRED_MESSAGE:
                await interaction.response.send_message(message, view=RegisterNowView(), ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            return

        view = SlotRepeatView(interaction.user.id, bet=float(bet), spins=int(spins))
        await interaction.response.send_message(embed=embed, view=view, ephemeral=False)
