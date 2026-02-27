from __future__ import annotations

import hashlib
import json
import math

import discord
from discord import Interaction

from stockbot.config.runtime import get_app_config
from stockbot.db.database import get_connection
from stockbot.db.repositories import get_state_value, set_state_value

DAILY_CLOSE_RANK_BONUS_PERK_ID = 3_000_000_001
DISABLED_PERK_EFFECT_TARGETS = {
    "slot_bet_multiplier",
    "steal_chance",
    "steal_amount_min",
    "steal_amount_max",
}


def _perk_override_key(guild_id: int, user_id: int, perk_id: int) -> str:
    return f"perk_user_override:{guild_id}:{user_id}:{perk_id}"


def _cmp(actual: float, operator: str, expected: float) -> bool:
    if operator == ">":
        return actual > expected
    if operator == ">=":
        return actual >= expected
    if operator == "<":
        return actual < expected
    if operator == "<=":
        return actual <= expected
    if operator == "==":
        return abs(actual - expected) < 1e-9
    if operator == "!=":
        return abs(actual - expected) >= 1e-9
    return actual >= expected


def _normalize_req_type(raw: object) -> str:
    value = str(raw or "commodity_qty").strip().lower()
    if value == "any_single_commodities_qty":
        return "any_single_commodity_qty"
    return value


def _stack_count(actual: float, operator: str, expected: float) -> int:
    if expected <= 0:
        return 1
    if operator in {">", ">="}:
        return max(0, int(math.floor(actual / expected)))
    if _cmp(actual, operator, expected):
        return 1
    return 0


def _scaled_value(
    effect: dict,
    commodity_qty_by_name: dict[str, int],
) -> float:
    source = str(effect.get("scale_source", "none") or "none").strip().lower()
    if source == "commodity_qty":
        key = str(effect.get("scale_key", "") or "").strip().lower()
        return float(commodity_qty_by_name.get(key, 0))
    return 0.0


def _apply_effect_to_accumulators(
    effect: dict,
    *,
    stacks: int,
    commodity_qty_by_name: dict[str, int],
    tracked_stats: tuple[str, ...],
    perk_add: dict[str, float],
    perk_mul: dict[str, float],
    effect_display_parts: list[str],
) -> None:
    target_stat = str(effect.get("target_stat", "") or "").strip().lower()
    if target_stat in DISABLED_PERK_EFFECT_TARGETS:
        return
    if target_stat not in tracked_stats:
        return
    value_mode = str(effect.get("value_mode", "flat") or "flat").strip().lower()
    value = float(effect.get("value", 0) or 0.0)
    scale_factor = float(effect.get("scale_factor", 0) or 0.0)
    cap = float(effect.get("cap", 0) or 0.0)
    scaled = _scaled_value(effect, commodity_qty_by_name)

    if value_mode == "flat":
        contrib = value
        if cap > 0:
            contrib = max(-cap, min(cap, contrib))
        perk_add[target_stat] += contrib * stacks
        if abs(contrib) > 1e-9:
            sign = "+" if contrib >= 0 else "-"
            effect_display_parts.append(f"{target_stat} {sign}{abs(contrib):.2f}")
        return

    if value_mode == "multiplier":
        one = max(0.0, value)
        perk_mul[target_stat] *= one ** stacks
        if abs(one - 1.0) > 1e-9:
            effect_display_parts.append(f"{target_stat} x{one:.2f}")
        return

    if value_mode == "per_item":
        contrib = value + (scale_factor * scaled)
        if cap > 0:
            contrib = max(-cap, min(cap, contrib))
        perk_add[target_stat] += contrib * stacks
        base_str = ""
        if abs(value) > 1e-9:
            sign = "+" if value >= 0 else "-"
            base_str = f"{target_stat} {sign}{abs(value):.2f}"
        scale_str = ""
        if abs(scale_factor) > 1e-9:
            scale_str = f"x{scale_factor:.2f} per item after"
        if base_str and scale_str:
            effect_display_parts.append(f"{base_str}, {scale_str}")
        elif base_str:
            effect_display_parts.append(base_str)
        elif scale_str:
            effect_display_parts.append(f"{target_stat} {scale_str}")


def _coerce_effects(raw: object) -> list[dict]:
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [e for e in raw if isinstance(e, dict)]
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _coerce_effects(parsed)


def _coerce_level_effects(raw: object, max_level: int) -> list[list[dict]]:
    out: list[list[dict]] = [[] for _ in range(max(1, int(max_level)))]
    text = str(raw or "").strip()
    if not text:
        return out
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return out
    if isinstance(parsed, list):
        for idx in range(min(len(parsed), len(out))):
            out[idx] = _coerce_effects(parsed[idx])
    return out


def apply_income_perks(
    guild_id: int,
    user_id: int,
    base_income: float,
) -> tuple[float, dict]:
    result = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=base_income,
        base_trade_limits=0,
    )
    return float(result["final"]["income"]), {"matched_perks": list(result["matched_perks"])}


def evaluate_user_perks(
    guild_id: int,
    user_id: int,
    *,
    base_income: float = 0.0,
    base_trade_limits: int | None = None,
    base_networth: float | None = None,
    base_commodities_limit: int | None = None,
    base_job_slots: int | None = None,
    base_timed_duration: float | None = None,
    base_chance_roll_bonus: float | None = None,
    base_slot_bet_multiplier: float | None = None,
    base_slot_bet_limit: float | None = None,
    base_slot_win_multiplier: float | None = None,
    base_slot_hourly_spin_limit: float | None = None,
    base_steal_chance: float | None = None,
    base_steal_amount_min: float | None = None,
    base_steal_amount_max: float | None = None,
) -> dict:
    """
    Evaluate perk requirements and aggregate effect modifiers for supported stats:
    - income
    - trade_limits
    - networth
    - commodities_limit
    - job_slots
    - timed_duration
    - chance_roll_bonus
    - slot_bet_multiplier
    - slot_bet_limit
    - slot_win_multiplier
    - slot_hourly_spin_limit
    - steal_chance
    - steal_amount_min
    - steal_amount_max
    """
    if base_trade_limits is None:
        base_trade_limits = int(get_app_config("TRADING_LIMITS"))
    if base_commodities_limit is None:
        base_commodities_limit = int(get_app_config("COMMODITIES_LIMIT"))
    if base_job_slots is None:
        base_job_slots = 1
    if base_timed_duration is None:
        base_timed_duration = 0.0
    if base_chance_roll_bonus is None:
        base_chance_roll_bonus = 0.0
    if base_slot_bet_multiplier is None:
        base_slot_bet_multiplier = 1.0
    if base_slot_bet_limit is None:
        base_slot_bet_limit = float(get_app_config("SLOT_BET_MAX"))
    if base_slot_win_multiplier is None:
        base_slot_win_multiplier = 1.0
    if base_slot_hourly_spin_limit is None:
        base_slot_hourly_spin_limit = float(get_app_config("SLOT_HOURLY_SPIN_LIMIT"))
    if base_steal_chance is None:
        base_steal_chance = 0.25
    if base_steal_amount_min is None:
        base_steal_amount_min = 50.0
    if base_steal_amount_max is None:
        base_steal_amount_max = 100.0

    with get_connection() as conn:
        holdings_rows = conn.execute(
            """
            SELECT commodity_name, quantity
            FROM user_commodities
            WHERE guild_id = ? AND user_id = ? AND quantity > 0
            """,
            (guild_id, user_id),
        ).fetchall()
        commodity_qty_by_name: dict[str, int] = {}
        for r in holdings_rows:
            key = str(r["commodity_name"]).strip().lower()
            if not key:
                continue
            commodity_qty_by_name[key] = commodity_qty_by_name.get(key, 0) + int(r["quantity"] or 0)
        tag_qty_rows = conn.execute(
            """
            SELECT ct.tag AS tag, COALESCE(SUM(uc.quantity), 0) AS qty
            FROM commodity_tags ct
            LEFT JOIN user_commodities uc
              ON uc.guild_id = ct.guild_id
             AND uc.user_id = ?
             AND uc.commodity_name = ct.commodity_name COLLATE NOCASE
            WHERE ct.guild_id = ?
            GROUP BY ct.tag
            """,
            (user_id, guild_id),
        ).fetchall()
        qty_by_tag = {
            str(r["tag"]).strip().lower(): int(r["qty"] or 0)
            for r in tag_qty_rows
            if str(r["tag"]).strip()
        }
        commodity_perk_rows = conn.execute(
            """
            SELECT c.name AS commodity_name, CAST(SUM(uc.quantity) AS INTEGER) AS quantity,
                   c.perk_name, c.perk_description, c.perk_min_qty, c.perk_effects_json
            FROM user_commodities uc
            JOIN commodities c
              ON c.guild_id = uc.guild_id
             AND c.name = uc.commodity_name COLLATE NOCASE
            WHERE uc.guild_id = ? AND uc.user_id = ? AND uc.quantity > 0
            GROUP BY c.name, c.perk_name, c.perk_description, c.perk_min_qty, c.perk_effects_json
            """,
            (guild_id, user_id),
        ).fetchall()
        property_rows = conn.execute(
            """
            SELECT up.property_id, up.level, up.ascended,
                   p.name, p.max_level, p.level_effects_json, p.ascension_effects_json
            FROM user_properties up
            JOIN properties p
              ON p.id = up.property_id
             AND p.guild_id = up.guild_id
            WHERE up.guild_id = ? AND up.user_id = ?
            """,
            (guild_id, user_id),
        ).fetchall()
        if base_networth is None:
            networth_row = conn.execute(
                """
                SELECT COALESCE(SUM(uc.quantity * c.price), 0.0) AS total
                FROM user_commodities uc
                JOIN commodities c
                  ON c.guild_id = uc.guild_id
                 AND c.name = uc.commodity_name COLLATE NOCASE
                WHERE uc.guild_id = ? AND uc.user_id = ?
                """,
                (guild_id, user_id),
            ).fetchone()
            base_networth = float(networth_row["total"]) if networth_row is not None else 0.0

        perks_rows = conn.execute(
            """
            SELECT id, name, description, enabled, priority, stack_mode, max_stacks
            FROM perks
            WHERE guild_id = ? AND enabled = 1
            ORDER BY priority ASC, id ASC
            """,
            (guild_id,),
        ).fetchall()
        perks = [dict(r) for r in perks_rows]
        req_rows = []
        effect_rows = []
        if perks:
            perk_ids = [int(p["id"]) for p in perks]
            placeholders = ",".join(["?"] * len(perk_ids))

            req_rows = conn.execute(
                f"""
                SELECT id, perk_id, group_id, req_type, commodity_name, operator, value
                FROM perk_requirements
                WHERE perk_id IN ({placeholders})
                ORDER BY perk_id, group_id, id
                """,
                tuple(perk_ids),
            ).fetchall()
            effect_rows = conn.execute(
                f"""
                SELECT id, perk_id, effect_type, target_stat, value_mode, value, scale_source, scale_key, scale_factor, cap
                FROM perk_effects
                WHERE perk_id IN ({placeholders})
                ORDER BY perk_id, id
                """,
                tuple(perk_ids),
            ).fetchall()
        override_rows = conn.execute(
            """
            SELECT key, value
            FROM app_state
            WHERE key LIKE ?
            """,
            (f"perk_user_override:{guild_id}:{user_id}:%",),
        ).fetchall()

    override_by_perk: dict[int, str] = {}
    for row in override_rows:
        key = str(row["key"] or "")
        value = str(row["value"] or "").strip().lower()
        if value not in {"auto", "on", "off"}:
            continue
        try:
            perk_id = int(key.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            continue
        if perk_id > 0:
            override_by_perk[perk_id] = value

    req_by_perk: dict[int, list[dict]] = {}
    for row in req_rows:
        req_by_perk.setdefault(int(row["perk_id"]), []).append(dict(row))
    effects_by_perk: dict[int, list[dict]] = {}
    for row in effect_rows:
        effects_by_perk.setdefault(int(row["perk_id"]), []).append(dict(row))

    tracked_stats = (
        "income",
        "trade_limits",
        "networth",
        "commodities_limit",
        "job_slots",
        "timed_duration",
        "chance_roll_bonus",
        "slot_bet_multiplier",
        "slot_bet_limit",
        "slot_win_multiplier",
        "slot_hourly_spin_limit",
        "steal_chance",
        "steal_amount_min",
        "steal_amount_max",
    )
    stat_add = {k: 0.0 for k in tracked_stats}
    stat_mul = {k: 1.0 for k in tracked_stats}
    matched: list[dict] = []

    for perk in perks:
        perk_id = int(perk["id"])
        override_mode = override_by_perk.get(perk_id, "auto")
        if override_mode == "off":
            continue
        stack_mode = str(perk.get("stack_mode", "add") or "add").strip().lower()
        max_stacks = max(1, int(perk.get("max_stacks", 1) or 1))

        if override_mode == "on":
            matched_group_stacks = [1]
        else:
            reqs = req_by_perk.get(perk_id, [])
            group_rows: dict[int, list[dict]] = {}
            for req in reqs:
                group_rows.setdefault(int(req.get("group_id", 1) or 1), []).append(req)

            matched_group_stacks: list[int] = []
            for _group_id, rows in group_rows.items():
                group_ok = True
                stack_candidates: list[int] = []
                for req in rows:
                    req_type = _normalize_req_type(req.get("req_type", "commodity_qty"))
                    op = str(req.get("operator", ">=") or ">=").strip()
                    expected = int(float(req.get("value", 1) or 1))
                    if expected < 0:
                        expected = 0
                    actual = 0.0
                    if req_type == "commodity_qty":
                        c_name = str(req.get("commodity_name", "") or "").strip().lower()
                        actual = float(commodity_qty_by_name.get(c_name, 0))
                    elif req_type == "tag_qty":
                        tag = str(req.get("commodity_name", "") or "").strip().lower()
                        actual = float(qty_by_tag.get(tag, 0))
                    elif req_type == "any_single_commodity_qty":
                        actual = float(max(commodity_qty_by_name.values()) if commodity_qty_by_name else 0)
                    else:
                        group_ok = False
                        break

                    if not _cmp(actual, op, expected):
                        group_ok = False
                        break
                    stack_candidates.append(_stack_count(actual, op, expected))

                if group_ok:
                    group_stack = min(stack_candidates) if stack_candidates else 1
                    matched_group_stacks.append(max(1, group_stack))

            # No requirements means always active.
            if not group_rows:
                matched_group_stacks = [1]

        if not matched_group_stacks:
            continue

        stacks = min(max(matched_group_stacks), max_stacks)
        if stack_mode in {"override", "max_only"}:
            stacks = 1

        perk_add = {k: 0.0 for k in tracked_stats}
        perk_mul = {k: 1.0 for k in tracked_stats}
        effect_display_parts: list[str] = []
        for effect in effects_by_perk.get(perk_id, []):
            _apply_effect_to_accumulators(
                effect,
                stacks=stacks,
                commodity_qty_by_name=commodity_qty_by_name,
                tracked_stats=tracked_stats,
                perk_add=perk_add,
                perk_mul=perk_mul,
                effect_display_parts=effect_display_parts,
            )

        has_change = any(abs(perk_add[k]) > 1e-9 or abs(perk_mul[k] - 1.0) > 1e-9 for k in tracked_stats)
        if not has_change:
            continue

        for key in tracked_stats:
            stat_add[key] += perk_add[key]
            stat_mul[key] *= perk_mul[key]
        matched.append(
            {
                "perk_id": perk_id,
                "name": str(perk.get("name", f"perk#{perk_id}")),
                "description": str(perk.get("description", "")),
                "stacks": stacks,
                "override_mode": override_mode,
                "add": perk_add["income"],
                "mul": perk_mul["income"],
                "adds": dict(perk_add),
                "muls": dict(perk_mul),
                "display": " | ".join(effect_display_parts),
            }
        )

    # Commodity-level per-item perks.
    for row in commodity_perk_rows:
        qty = max(0, int(row["quantity"] or 0))
        if qty <= 0:
            continue
        min_qty = max(1, int(row["perk_min_qty"] or 1))
        if qty < min_qty:
            continue
        effects_raw = str(row["perk_effects_json"] or "").strip()
        if not effects_raw:
            continue
        try:
            parsed = json.loads(effects_raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            effects = [parsed]
        elif isinstance(parsed, list):
            effects = [e for e in parsed if isinstance(e, dict)]
        else:
            effects = []
        if not effects:
            continue

        stacks = max(1, qty // min_qty)
        perk_add = {k: 0.0 for k in tracked_stats}
        perk_mul = {k: 1.0 for k in tracked_stats}
        effect_display_parts: list[str] = []
        for effect in effects:
            if not effect.get("scale_key"):
                effect = {
                    **effect,
                    "scale_key": str(row["commodity_name"]).strip().lower(),
                }
            _apply_effect_to_accumulators(
                effect,
                stacks=stacks,
                commodity_qty_by_name=commodity_qty_by_name,
                tracked_stats=tracked_stats,
                perk_add=perk_add,
                perk_mul=perk_mul,
                effect_display_parts=effect_display_parts,
            )

        has_change = any(abs(perk_add[k]) > 1e-9 or abs(perk_mul[k] - 1.0) > 1e-9 for k in tracked_stats)
        if not has_change:
            continue

        for key in tracked_stats:
            stat_add[key] += perk_add[key]
            stat_mul[key] *= perk_mul[key]

        commodity_name = str(row["commodity_name"] or "").strip()
        derived_id = 2_000_000_000 + (int(hashlib.md5(commodity_name.lower().encode("utf-8")).hexdigest()[:8], 16) % 100_000_000)
        perk_name = str(row["perk_name"] or "").strip() or f"{commodity_name} perk"
        matched.append(
            {
                "perk_id": int(derived_id),
                "name": perk_name,
                "description": str(row["perk_description"] or "").strip(),
                "stacks": stacks,
                "add": perk_add["income"],
                "mul": perk_mul["income"],
                "adds": dict(perk_add),
                "muls": dict(perk_mul),
                "display": " | ".join(effect_display_parts),
            }
        )

    # Property effects (level effects + permanent ascension effects).
    for row in property_rows:
        row_data = dict(row)
        level = max(0, int(row_data.get("level", 0) or 0))
        ascended = int(row_data.get("ascended", 0) or 0) > 0
        max_level = max(1, int(row_data.get("max_level", 5) or 5))
        property_name = str(row_data.get("name", f"Property #{row_data.get('property_id', '?')}"))

        level_effects = _coerce_level_effects(row_data.get("level_effects_json", ""), max_level)
        current_level_effects = level_effects[level - 1] if 1 <= level <= len(level_effects) else []
        ascension_effects = _coerce_effects(row_data.get("ascension_effects_json", ""))

        for source_name, effects in (
            (f"{property_name} L{level}", current_level_effects),
            (f"{property_name} Ascension", ascension_effects if ascended else []),
        ):
            if not effects:
                continue
            perk_add = {k: 0.0 for k in tracked_stats}
            perk_mul = {k: 1.0 for k in tracked_stats}
            effect_display_parts: list[str] = []
            for effect in effects:
                _apply_effect_to_accumulators(
                    effect,
                    stacks=1,
                    commodity_qty_by_name=commodity_qty_by_name,
                    tracked_stats=tracked_stats,
                    perk_add=perk_add,
                    perk_mul=perk_mul,
                    effect_display_parts=effect_display_parts,
                )
            has_change = any(abs(perk_add[k]) > 1e-9 or abs(perk_mul[k] - 1.0) > 1e-9 for k in tracked_stats)
            if not has_change:
                continue
            for key in tracked_stats:
                stat_add[key] += perk_add[key]
                stat_mul[key] *= perk_mul[key]
            derived_id = 4_000_000_000 + int(row_data.get("property_id", 0) or 0)
            if "Ascension" in source_name:
                derived_id += 100_000_000
            matched.append(
                {
                    "perk_id": int(derived_id),
                    "name": source_name,
                    "description": "Property effect",
                    "stacks": 1,
                    "add": perk_add["income"],
                    "mul": perk_mul["income"],
                    "adds": dict(perk_add),
                    "muls": dict(perk_mul),
                    "display": " | ".join(effect_display_parts),
                }
            )

    # Daily close rank bonus (top 3) stored in app_state and active until next close.
    daily_bonus_raw = get_state_value(f"daily_networth_bonus:{guild_id}:{user_id}")
    try:
        daily_networth_bonus = float(daily_bonus_raw) if daily_bonus_raw is not None else 0.0
    except (TypeError, ValueError):
        daily_networth_bonus = 0.0
    if abs(daily_networth_bonus) > 1e-9:
        stat_add["networth"] += daily_networth_bonus
        matched.append(
            {
                "perk_id": DAILY_CLOSE_RANK_BONUS_PERK_ID,
                "name": "Daily Close Rank Bonus",
                "description": "Top close ranking bonus active until next close.",
                "stacks": 1,
                "add": 0.0,
                "mul": 1.0,
                "adds": {
                    **{k: 0.0 for k in tracked_stats},
                    "networth": daily_networth_bonus,
                },
                "muls": {k: 1.0 for k in tracked_stats},
                "display": f"networth +{daily_networth_bonus:.2f}",
            }
        )

    # Allow negative income effects; downstream payout logic can route deficits to owe.
    final_income = (float(base_income) + stat_add["income"]) * stat_mul["income"]
    final_trade_limits = max(0.0, (float(base_trade_limits) + stat_add["trade_limits"]) * stat_mul["trade_limits"])
    final_networth = max(0.0, (float(base_networth) + stat_add["networth"]) * stat_mul["networth"])
    final_commodities_limit = max(
        0.0,
        (float(base_commodities_limit) + stat_add["commodities_limit"]) * stat_mul["commodities_limit"],
    )
    final_job_slots = max(0.0, (float(base_job_slots) + stat_add["job_slots"]) * stat_mul["job_slots"])
    final_timed_duration = max(
        0.1,
        (float(base_timed_duration) + stat_add["timed_duration"]) * stat_mul["timed_duration"],
    )
    final_chance_roll_bonus = (
        float(base_chance_roll_bonus) + stat_add["chance_roll_bonus"]
    ) * stat_mul["chance_roll_bonus"]
    final_slot_bet_multiplier = max(
        0.01,
        (float(base_slot_bet_multiplier) + stat_add["slot_bet_multiplier"]) * stat_mul["slot_bet_multiplier"],
    )
    final_slot_bet_limit = max(
        0.0,
        (float(base_slot_bet_limit) + stat_add["slot_bet_limit"]) * stat_mul["slot_bet_limit"],
    )
    final_slot_win_multiplier = max(
        0.0,
        (float(base_slot_win_multiplier) + stat_add["slot_win_multiplier"]) * stat_mul["slot_win_multiplier"],
    )
    final_slot_hourly_spin_limit = max(
        0.0,
        (float(base_slot_hourly_spin_limit) + stat_add["slot_hourly_spin_limit"]) * stat_mul["slot_hourly_spin_limit"],
    )
    final_steal_chance = max(
        0.0,
        min(
            1.0,
            (float(base_steal_chance) + stat_add["steal_chance"]) * stat_mul["steal_chance"],
        ),
    )
    final_steal_amount_min = max(
        0.0,
        (float(base_steal_amount_min) + stat_add["steal_amount_min"]) * stat_mul["steal_amount_min"],
    )
    final_steal_amount_max = max(
        0.0,
        (float(base_steal_amount_max) + stat_add["steal_amount_max"]) * stat_mul["steal_amount_max"],
    )
    if final_steal_amount_min > final_steal_amount_max:
        final_steal_amount_min, final_steal_amount_max = final_steal_amount_max, final_steal_amount_min
    return {
        "base": {
            "income": float(base_income),
            "trade_limits": float(base_trade_limits),
            "networth": float(base_networth),
            "commodities_limit": float(base_commodities_limit),
            "job_slots": float(base_job_slots),
            "timed_duration": float(base_timed_duration),
            "chance_roll_bonus": float(base_chance_roll_bonus),
            "slot_bet_multiplier": float(base_slot_bet_multiplier),
            "slot_bet_limit": float(base_slot_bet_limit),
            "slot_win_multiplier": float(base_slot_win_multiplier),
            "slot_hourly_spin_limit": float(base_slot_hourly_spin_limit),
            "steal_chance": float(base_steal_chance),
            "steal_amount_min": float(base_steal_amount_min),
            "steal_amount_max": float(base_steal_amount_max),
        },
        "final": {
            "income": float(final_income),
            "trade_limits": float(int(round(final_trade_limits))),
            "networth": float(final_networth),
            "commodities_limit": float(int(round(final_commodities_limit))),
            "job_slots": float(int(round(final_job_slots))),
            "timed_duration": float(final_timed_duration),
            "chance_roll_bonus": float(final_chance_roll_bonus),
            "slot_bet_multiplier": float(final_slot_bet_multiplier),
            "slot_bet_limit": float(final_slot_bet_limit),
            "slot_win_multiplier": float(final_slot_win_multiplier),
            "slot_hourly_spin_limit": float(int(round(final_slot_hourly_spin_limit))),
            "steal_chance": float(final_steal_chance),
            "steal_amount_min": float(final_steal_amount_min),
            "steal_amount_max": float(final_steal_amount_max),
        },
        "matched_perks": matched,
    }


def _perk_state_key(guild_id: int, user_id: int, perk_id: int) -> str:
    return f"perk_active:{guild_id}:{user_id}:{perk_id}"


async def _pick_announcement_channel(
    guild: discord.Guild,
    client: discord.Client,
) -> discord.abc.Messageable | None:
    preferred_channel_id = int(get_app_config("ANNOUNCEMENT_CHANNEL_ID"))
    if preferred_channel_id > 0:
        channel = guild.get_channel(preferred_channel_id) or client.get_channel(preferred_channel_id)
        if channel is None:
            try:
                channel = await guild.fetch_channel(preferred_channel_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                channel = None
        if isinstance(channel, (discord.TextChannel, discord.Thread)) and channel.guild.id == guild.id:
            return channel

    if guild.system_channel is not None:
        return guild.system_channel
    for channel in guild.text_channels:
        return channel
    return None


async def check_and_announce_perk_activations(
    interaction: Interaction,
    target_user_id: int | None = None,
) -> list[str]:
    if interaction.guild is None:
        return []
    return await check_and_announce_perk_activations_for_user(
        client=interaction.client,
        guild_id=interaction.guild.id,
        user_id=int(target_user_id or interaction.user.id),
    )

async def check_and_announce_perk_activations_for_user(
    *,
    client: discord.Client,
    guild_id: int,
    user_id: int,
) -> list[str]:
    guild = client.get_guild(guild_id)
    if guild is None:
        try:
            guild = await client.fetch_guild(guild_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return []

    eval_result = evaluate_user_perks(guild_id, user_id, base_income=0.0)
    matched = list(eval_result.get("matched_perks", []))
    current_ids = {int(row.get("perk_id", 0)) for row in matched if int(row.get("perk_id", 0)) > 0}
    current_names = {
        int(row.get("perk_id", 0)): str(row.get("name", "Unknown"))
        for row in matched
        if int(row.get("perk_id", 0)) > 0
    }
    current_descriptions = {
        int(row.get("perk_id", 0)): str(row.get("description", "")).strip()
        for row in matched
        if int(row.get("perk_id", 0)) > 0
    }
    current_displays = {
        int(row.get("perk_id", 0)): str(row.get("display", "")).strip()
        for row in matched
        if int(row.get("perk_id", 0)) > 0
    }

    prefix = f"perk_active:{guild_id}:{user_id}:"
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT key, value
            FROM app_state
            WHERE key LIKE ?
            """,
            (f"{prefix}%",),
        ).fetchall()
    active_ids: set[int] = set()
    for row in rows:
        key = str(row["key"])
        val = str(row["value"])
        if val != "1":
            continue
        try:
            active_ids.add(int(key.rsplit(":", 1)[-1]))
        except (TypeError, ValueError):
            continue

    newly_activated_ids = [perk_id for perk_id in sorted(current_ids) if perk_id not in active_ids]

    for perk_id in current_ids:
        set_state_value(_perk_state_key(guild_id, user_id, perk_id), "1")
    for perk_id in active_ids:
        if perk_id not in current_ids:
            set_state_value(_perk_state_key(guild_id, user_id, perk_id), "0")

    if not newly_activated_ids:
        return []

    perk_names = [current_names.get(perk_id, f"perk#{perk_id}") for perk_id in newly_activated_ids]
    channel = await _pick_announcement_channel(guild, client)
    if channel is None:
        return perk_names
    blocks: list[str] = []
    for perk_id, perk_name in zip(newly_activated_ids, perk_names):
        desc = current_descriptions.get(perk_id) or "No description."
        modifier = current_displays.get(perk_id) or "No modifier."
        blocks.append(
            "**PERK ACTIVATED!!**:\n"
            f"**{perk_name}**\n"
            f"*{desc}*\n"
            "**MODIFIERS**:\n"
            f"{modifier}"
        )
    try:
        await channel.send(f"<@{user_id}>\n" + "\n\n".join(blocks))
    except Exception as exc:
        print(f"[perks] failed to send activation announcement for guild={guild_id} user={user_id}: {exc}")
    return perk_names
