from __future__ import annotations

import json
from datetime import datetime, timezone

from stockbot.db import add_action_history, get_connection, get_user, update_user_bank


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


def _coerce_level_costs(raw: object, max_level: int) -> list[float]:
    out = [0.0 for _ in range(max(1, max_level))]
    try:
        parsed = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        parsed = []
    if isinstance(parsed, list):
        for idx in range(min(len(parsed), len(out))):
            try:
                out[idx] = max(0.0, float(parsed[idx]))
            except (TypeError, ValueError):
                out[idx] = 0.0
    return out


def _coerce_level_effects(raw: object, max_level: int) -> list[list[dict]]:
    out: list[list[dict]] = [[] for _ in range(max(1, max_level))]
    try:
        parsed = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        parsed = []
    if isinstance(parsed, list):
        for idx in range(min(len(parsed), len(out))):
            out[idx] = _coerce_effects(parsed[idx])
    return out


def get_properties(guild_id: int, enabled_only: bool = False) -> list[dict]:
    with get_connection() as conn:
        if enabled_only:
            rows = conn.execute(
                """
                SELECT id, guild_id, name, description, image_url, enabled, max_level, buy_price,
                       level_costs_json, level_effects_json, ascension_cost, ascension_effects_json, updated_at
                FROM properties
                WHERE guild_id = ? AND enabled = 1
                ORDER BY name ASC
                """,
                (guild_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, guild_id, name, description, image_url, enabled, max_level, buy_price,
                       level_costs_json, level_effects_json, ascension_cost, ascension_effects_json, updated_at
                FROM properties
                WHERE guild_id = ?
                ORDER BY name ASC
                """,
                (guild_id,),
            ).fetchall()
    out: list[dict] = []
    for row in rows:
        item = dict(row)
        max_level = max(1, int(item.get("max_level", 5) or 5))
        item["max_level"] = max_level
        item["level_costs"] = _coerce_level_costs(item.get("level_costs_json", "[]"), max_level)
        item["level_effects"] = _coerce_level_effects(item.get("level_effects_json", "[]"), max_level)
        item["ascension_effects"] = _coerce_effects(item.get("ascension_effects_json", "[]"))
        out.append(item)
    return out


def get_property(guild_id: int, property_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, guild_id, name, description, image_url, enabled, max_level, buy_price,
                   level_costs_json, level_effects_json, ascension_cost, ascension_effects_json, updated_at
            FROM properties
            WHERE guild_id = ? AND id = ?
            """,
            (guild_id, int(property_id)),
        ).fetchone()
    if row is None:
        return None
    item = dict(row)
    max_level = max(1, int(item.get("max_level", 5) or 5))
    item["max_level"] = max_level
    item["level_costs"] = _coerce_level_costs(item.get("level_costs_json", "[]"), max_level)
    item["level_effects"] = _coerce_level_effects(item.get("level_effects_json", "[]"), max_level)
    item["ascension_effects"] = _coerce_effects(item.get("ascension_effects_json", "[]"))
    return item


def get_user_property_states(guild_id: int, user_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT up.guild_id, up.user_id, up.property_id, up.level, up.ascended, up.ascended_count, up.updated_at,
                   p.name, p.description, p.image_url, p.max_level, p.buy_price, p.enabled,
                   p.level_costs_json, p.level_effects_json, p.ascension_cost, p.ascension_effects_json
            FROM user_properties up
            JOIN properties p
              ON p.id = up.property_id
             AND p.guild_id = up.guild_id
            WHERE up.guild_id = ? AND up.user_id = ?
            ORDER BY p.name ASC
            """,
            (guild_id, user_id),
        ).fetchall()
    out: list[dict] = []
    for row in rows:
        item = dict(row)
        max_level = max(1, int(item.get("max_level", 5) or 5))
        item["max_level"] = max_level
        item["level"] = max(0, int(item.get("level", 0) or 0))
        item["ascended"] = 1 if int(item.get("ascended", 0) or 0) > 0 else 0
        item["level_costs"] = _coerce_level_costs(item.get("level_costs_json", "[]"), max_level)
        item["level_effects"] = _coerce_level_effects(item.get("level_effects_json", "[]"), max_level)
        item["ascension_effects"] = _coerce_effects(item.get("ascension_effects_json", "[]"))
        out.append(item)
    return out


def _upsert_user_property_state(
    guild_id: int,
    user_id: int,
    property_id: int,
    *,
    level: int,
    ascended: int,
    ascended_count: int,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO user_properties (
                guild_id, user_id, property_id, level, ascended, ascended_count, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, property_id)
            DO UPDATE SET
                level = excluded.level,
                ascended = excluded.ascended,
                ascended_count = excluded.ascended_count,
                updated_at = excluded.updated_at
            """,
            (
                guild_id,
                user_id,
                property_id,
                int(level),
                1 if int(ascended) > 0 else 0,
                max(0, int(ascended_count)),
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def _pay(guild_id: int, user_id: int, amount: float) -> tuple[bool, float]:
    user = get_user(guild_id, user_id)
    if user is None:
        return False, 0.0
    cost = max(0.0, float(amount))
    bank = float(user.get("bank", 0.0))
    if bank + 1e-9 < cost:
        return False, bank
    update_user_bank(guild_id, user_id, bank - cost)
    return True, bank - cost


def buy_property(guild_id: int, user_id: int, property_id: int) -> tuple[bool, str]:
    prop = get_property(guild_id, property_id)
    if prop is None or int(prop.get("enabled", 0) or 0) != 1:
        return False, "Property not available."
    owned_rows = get_user_property_states(guild_id, user_id)
    existing = next((r for r in owned_rows if int(r.get("property_id", 0)) == int(property_id)), None)
    if existing is not None and (int(existing.get("level", 0)) > 0 or int(existing.get("ascended", 0)) > 0):
        return False, "You already own this property."

    buy_price = max(0.0, float(prop.get("buy_price", 0.0) or 0.0))
    ok, bank_after = _pay(guild_id, user_id, buy_price)
    if not ok:
        return False, f"Not enough funds. Need ${buy_price:.2f}."
    _upsert_user_property_state(
        guild_id,
        user_id,
        int(property_id),
        level=1,
        ascended=0,
        ascended_count=int(existing.get("ascended_count", 0)) if existing else 0,
    )
    add_action_history(
        guild_id=guild_id,
        user_id=user_id,
        action_type="property_buy",
        target_type="property",
        target_symbol=str(prop.get("name", f"property#{property_id}")),
        quantity=1.0,
        unit_price=buy_price,
        total_amount=buy_price,
        details="level=1",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Purchased **{prop['name']}** at Level 1 for **${buy_price:.2f}**. Balance: ${bank_after:.2f}."


def upgrade_property(guild_id: int, user_id: int, property_id: int) -> tuple[bool, str]:
    prop = get_property(guild_id, property_id)
    if prop is None:
        return False, "Property not found."
    state_rows = get_user_property_states(guild_id, user_id)
    state = next((r for r in state_rows if int(r.get("property_id", 0)) == int(property_id)), None)
    if state is None or int(state.get("level", 0)) <= 0:
        return False, "You do not own this property yet."
    if int(state.get("ascended", 0)) > 0:
        return False, "This property is already ascended."
    level = int(state.get("level", 0))
    max_level = int(prop.get("max_level", 5))
    if level >= max_level:
        return False, "Property is already max level."
    level_costs = list(prop.get("level_costs", []))
    target_level = level + 1
    idx = target_level - 1
    cost = float(level_costs[idx] if idx < len(level_costs) else 0.0)
    ok, bank_after = _pay(guild_id, user_id, cost)
    if not ok:
        return False, f"Not enough funds. Need ${cost:.2f}."
    _upsert_user_property_state(
        guild_id,
        user_id,
        int(property_id),
        level=target_level,
        ascended=0,
        ascended_count=int(state.get("ascended_count", 0)),
    )
    add_action_history(
        guild_id=guild_id,
        user_id=user_id,
        action_type="property_upgrade",
        target_type="property",
        target_symbol=str(prop.get("name", f"property#{property_id}")),
        quantity=1.0,
        unit_price=cost,
        total_amount=cost,
        details=f"level={target_level}",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Upgraded **{prop['name']}** to Level {target_level}/{max_level} for **${cost:.2f}**. Balance: ${bank_after:.2f}."


def ascend_property(guild_id: int, user_id: int, property_id: int) -> tuple[bool, str]:
    prop = get_property(guild_id, property_id)
    if prop is None:
        return False, "Property not found."
    state_rows = get_user_property_states(guild_id, user_id)
    state = next((r for r in state_rows if int(r.get("property_id", 0)) == int(property_id)), None)
    if state is None or int(state.get("level", 0)) <= 0:
        return False, "You do not own this property yet."
    if int(state.get("ascended", 0)) > 0:
        return False, "Property is already ascended."
    max_level = int(prop.get("max_level", 5))
    level = int(state.get("level", 0))
    if level < max_level:
        return False, f"Reach Level {max_level} before ascending."
    cost = max(0.0, float(prop.get("ascension_cost", 0.0) or 0.0))
    ok, bank_after = _pay(guild_id, user_id, cost)
    if not ok:
        return False, f"Not enough funds. Need ${cost:.2f}."
    _upsert_user_property_state(
        guild_id,
        user_id,
        int(property_id),
        level=0,
        ascended=1,
        ascended_count=int(state.get("ascended_count", 0)) + 1,
    )
    add_action_history(
        guild_id=guild_id,
        user_id=user_id,
        action_type="property_ascend",
        target_type="property",
        target_symbol=str(prop.get("name", f"property#{property_id}")),
        quantity=1.0,
        unit_price=cost,
        total_amount=cost,
        details="ascension=1",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    return True, f"Ascended **{prop['name']}**. Permanent effects are now active. Cost: **${cost:.2f}**. Balance: ${bank_after:.2f}."

