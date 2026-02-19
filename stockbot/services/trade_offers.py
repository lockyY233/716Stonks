from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone

from stockbot.config.runtime import get_app_config
from stockbot.db.database import get_connection
from stockbot.db.repositories import recalc_user_networth
from stockbot.services.perks import evaluate_user_perks


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _offer_key(trade_id: str) -> str:
    return f"trade_offer:{trade_id}"


def _normalize_items(raw: object) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for k, v in raw.items():
        name = str(k or "").strip()
        if not name:
            continue
        try:
            qty = int(v)
        except (TypeError, ValueError):
            continue
        if qty <= 0:
            continue
        out[name] = qty
    return out


def _load_offer_row(conn, trade_id: str):
    return conn.execute(
        "SELECT value FROM app_state WHERE key = ?",
        (_offer_key(trade_id),),
    ).fetchone()


def _parse_offer(raw: str) -> dict | None:
    try:
        data = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    data["requester_items"] = _normalize_items(data.get("requester_items", {}))
    data["target_items"] = _normalize_items(data.get("target_items", {}))
    try:
        data["requester_money"] = max(0.0, float(data.get("requester_money", 0.0)))
    except (TypeError, ValueError):
        data["requester_money"] = 0.0
    try:
        data["target_money"] = max(0.0, float(data.get("target_money", 0.0)))
    except (TypeError, ValueError):
        data["target_money"] = 0.0
    return data


def create_trade_offer(guild_id: int, requester_id: int, target_id: int, ttl_seconds: int = 3600) -> dict:
    now_iso = _now_iso()
    created_at_epoch = int(datetime.now(timezone.utc).timestamp())
    expires_at = created_at_epoch + max(60, int(ttl_seconds))
    with get_connection() as conn:
        trade_id = ""
        for _ in range(8):
            candidate = secrets.token_urlsafe(6).replace("-", "").replace("_", "")
            row = _load_offer_row(conn, candidate)
            if row is None:
                trade_id = candidate
                break
        if not trade_id:
            raise RuntimeError("Unable to create unique trade id")
        offer = {
            "id": trade_id,
            "guild_id": int(guild_id),
            "requester_id": int(requester_id),
            "target_id": int(target_id),
            "requester_money": 0.0,
            "target_money": 0.0,
            "requester_items": {},
            "target_items": {},
            "status": "draft",
            "pending_for": int(requester_id),
            "message_channel_id": 0,
            "message_id": 0,
            "created_at": now_iso,
            "updated_at": now_iso,
            "expires_at": int(expires_at),
        }
        conn.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (_offer_key(trade_id), json.dumps(offer, separators=(",", ":"))),
        )
        return offer


def get_trade_offer(trade_id: str) -> dict | None:
    with get_connection() as conn:
        row = _load_offer_row(conn, trade_id)
        if row is None:
            return None
    return _parse_offer(str(row["value"]))


def save_trade_offer(offer: dict) -> None:
    trade_id = str(offer.get("id", "")).strip()
    if not trade_id:
        raise ValueError("offer id missing")
    payload = dict(offer)
    payload["requester_items"] = _normalize_items(payload.get("requester_items", {}))
    payload["target_items"] = _normalize_items(payload.get("target_items", {}))
    payload["requester_money"] = max(0.0, float(payload.get("requester_money", 0.0)))
    payload["target_money"] = max(0.0, float(payload.get("target_money", 0.0)))
    payload["updated_at"] = _now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (_offer_key(trade_id), json.dumps(payload, separators=(",", ":"))),
        )


def delete_trade_offer(trade_id: str) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM app_state WHERE key = ?", (_offer_key(trade_id),))


def _user_commodity_qty_map(conn, guild_id: int, user_id: int) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT commodity_name, quantity
        FROM user_commodities
        WHERE guild_id = ? AND user_id = ? AND quantity > 0
        """,
        (guild_id, user_id),
    ).fetchall()
    return {str(r["commodity_name"]): int(r["quantity"]) for r in rows}


def _all_commodity_names(conn, guild_id: int) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM commodities WHERE guild_id = ?",
        (guild_id,),
    ).fetchall()
    return {str(r["name"]) for r in rows}


def _effective_commodity_limit(guild_id: int, user_id: int) -> int:
    base_limit = int(get_app_config("COMMODITIES_LIMIT"))
    evaluated = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=0,
        base_networth=0.0,
        base_commodities_limit=base_limit,
        base_job_slots=1,
    )
    return int(evaluated["final"]["commodities_limit"])


def validate_trade_offer(offer: dict) -> tuple[bool, str]:
    guild_id = int(offer.get("guild_id", 0))
    requester_id = int(offer.get("requester_id", 0))
    target_id = int(offer.get("target_id", 0))
    if guild_id <= 0 or requester_id <= 0 or target_id <= 0:
        return False, "Invalid trade payload."
    if requester_id == target_id:
        return False, "You cannot trade with yourself."
    requester_items = _normalize_items(offer.get("requester_items", {}))
    target_items = _normalize_items(offer.get("target_items", {}))
    requester_money = max(0.0, float(offer.get("requester_money", 0.0)))
    target_money = max(0.0, float(offer.get("target_money", 0.0)))
    if requester_money <= 0 and target_money <= 0 and not requester_items and not target_items:
        return False, "Trade cannot be empty."
    with get_connection() as conn:
        req_user = conn.execute(
            "SELECT bank FROM users WHERE guild_id = ? AND user_id = ?",
            (guild_id, requester_id),
        ).fetchone()
        tgt_user = conn.execute(
            "SELECT bank FROM users WHERE guild_id = ? AND user_id = ?",
            (guild_id, target_id),
        ).fetchone()
        if req_user is None or tgt_user is None:
            return False, "Both users must be registered."
        if requester_money > float(req_user["bank"]):
            return False, "Requester does not have enough balance."
        if target_money > float(tgt_user["bank"]):
            return False, "Target user does not have enough balance."
        commodity_names = _all_commodity_names(conn, guild_id)
        req_holdings = _user_commodity_qty_map(conn, guild_id, requester_id)
        tgt_holdings = _user_commodity_qty_map(conn, guild_id, target_id)
        for name, qty in requester_items.items():
            if name not in commodity_names:
                return False, f"Commodity not found: {name}"
            if qty > int(req_holdings.get(name, 0)):
                return False, f"Requester lacks quantity for {name}."
        for name, qty in target_items.items():
            if name not in commodity_names:
                return False, f"Commodity not found: {name}"
            if qty > int(tgt_holdings.get(name, 0)):
                return False, f"Target user lacks quantity for {name}."

        # Enforce commodity cap after settlement for both sides.
        req_limit = _effective_commodity_limit(guild_id, requester_id)
        tgt_limit = _effective_commodity_limit(guild_id, target_id)
        req_owned_total = sum(max(0, int(qty)) for qty in req_holdings.values())
        tgt_owned_total = sum(max(0, int(qty)) for qty in tgt_holdings.values())
        req_sent_total = sum(max(0, int(qty)) for qty in requester_items.values())
        req_recv_total = sum(max(0, int(qty)) for qty in target_items.values())
        tgt_sent_total = sum(max(0, int(qty)) for qty in target_items.values())
        tgt_recv_total = sum(max(0, int(qty)) for qty in requester_items.values())
        req_after_total = req_owned_total - req_sent_total + req_recv_total
        tgt_after_total = tgt_owned_total - tgt_sent_total + tgt_recv_total
        if req_limit > 0 and req_after_total > req_limit:
            return False, (
                f"Requester would exceed commodity cap after trade "
                f"({req_after_total}/{req_limit})."
            )
        if tgt_limit > 0 and tgt_after_total > tgt_limit:
            return False, (
                f"Target user would exceed commodity cap after trade "
                f"({tgt_after_total}/{tgt_limit})."
            )
    return True, ""


def apply_trade_offer(offer: dict) -> tuple[bool, str]:
    ok, msg = validate_trade_offer(offer)
    if not ok:
        return False, msg
    guild_id = int(offer["guild_id"])
    requester_id = int(offer["requester_id"])
    target_id = int(offer["target_id"])
    requester_items = _normalize_items(offer.get("requester_items", {}))
    target_items = _normalize_items(offer.get("target_items", {}))
    requester_money = max(0.0, float(offer.get("requester_money", 0.0)))
    target_money = max(0.0, float(offer.get("target_money", 0.0)))
    now = _now_iso()

    with get_connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            req_user = conn.execute(
                "SELECT bank FROM users WHERE guild_id = ? AND user_id = ?",
                (guild_id, requester_id),
            ).fetchone()
            tgt_user = conn.execute(
                "SELECT bank FROM users WHERE guild_id = ? AND user_id = ?",
                (guild_id, target_id),
            ).fetchone()
            if req_user is None or tgt_user is None:
                conn.execute("ROLLBACK")
                return False, "Both users must be registered."
            req_bank = float(req_user["bank"])
            tgt_bank = float(tgt_user["bank"])
            if requester_money > req_bank or target_money > tgt_bank:
                conn.execute("ROLLBACK")
                return False, "Insufficient balance at settlement time."

            req_holdings = _user_commodity_qty_map(conn, guild_id, requester_id)
            tgt_holdings = _user_commodity_qty_map(conn, guild_id, target_id)
            for name, qty in requester_items.items():
                if qty > int(req_holdings.get(name, 0)):
                    conn.execute("ROLLBACK")
                    return False, f"Requester no longer has enough {name}."
            for name, qty in target_items.items():
                if qty > int(tgt_holdings.get(name, 0)):
                    conn.execute("ROLLBACK")
                    return False, f"Target no longer has enough {name}."

            new_req_bank = req_bank - requester_money + target_money
            new_tgt_bank = tgt_bank - target_money + requester_money
            conn.execute(
                "UPDATE users SET bank = ? WHERE guild_id = ? AND user_id = ?",
                (new_req_bank, guild_id, requester_id),
            )
            conn.execute(
                "UPDATE users SET bank = ? WHERE guild_id = ? AND user_id = ?",
                (new_tgt_bank, guild_id, target_id),
            )

            for name, qty in requester_items.items():
                conn.execute(
                    """
                    INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, commodity_name)
                    DO UPDATE SET quantity = quantity + excluded.quantity
                    """,
                    (guild_id, requester_id, name, -qty),
                )
                conn.execute(
                    """
                    INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, commodity_name)
                    DO UPDATE SET quantity = quantity + excluded.quantity
                    """,
                    (guild_id, target_id, name, qty),
                )
            for name, qty in target_items.items():
                conn.execute(
                    """
                    INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, commodity_name)
                    DO UPDATE SET quantity = quantity + excluded.quantity
                    """,
                    (guild_id, target_id, name, -qty),
                )
                conn.execute(
                    """
                    INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, commodity_name)
                    DO UPDATE SET quantity = quantity + excluded.quantity
                    """,
                    (guild_id, requester_id, name, qty),
                )

            conn.execute(
                "DELETE FROM user_commodities WHERE guild_id = ? AND quantity <= 0",
                (guild_id,),
            )

            req_details = {
                "trade_id": str(offer.get("id", "")),
                "counterparty_user_id": target_id,
                "money_sent": requester_money,
                "money_received": target_money,
                "items_sent": requester_items,
                "items_received": target_items,
            }
            tgt_details = {
                "trade_id": str(offer.get("id", "")),
                "counterparty_user_id": requester_id,
                "money_sent": target_money,
                "money_received": requester_money,
                "items_sent": target_items,
                "items_received": requester_items,
            }
            conn.execute(
                """
                INSERT INTO action_history (
                    guild_id, user_id, action_type, target_type, target_symbol,
                    quantity, unit_price, total_amount, details, created_at
                )
                VALUES (?, ?, 'trade', 'trade', ?, 0, 0, ?, ?, ?)
                """,
                (guild_id, requester_id, str(target_id), float(target_money - requester_money), json.dumps(req_details, separators=(",", ":")), now),
            )
            conn.execute(
                """
                INSERT INTO action_history (
                    guild_id, user_id, action_type, target_type, target_symbol,
                    quantity, unit_price, total_amount, details, created_at
                )
                VALUES (?, ?, 'trade', 'trade', ?, 0, 0, ?, ?, ?)
                """,
                (guild_id, target_id, str(requester_id), float(requester_money - target_money), json.dumps(tgt_details, separators=(",", ":")), now),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    recalc_user_networth(guild_id, requester_id)
    recalc_user_networth(guild_id, target_id)
    return True, "Trade settled successfully."
