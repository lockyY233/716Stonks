import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from stockbot.config import DB_PATH
from stockbot.db.database import get_connection, init_db


def register_user(
    guild_id: int,
    user_id: int,
    joined_at: str,
    bank: int,
    display_name: str,
    rank: str,
) -> bool:
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT 1 FROM users WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        if existing:
            return False

        conn.execute(
            """
            INSERT INTO users (guild_id, user_id, joined_at, bank, networth, owe, display_name, rank)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (guild_id, user_id, joined_at, bank, 0.0, 0.0, display_name, rank),
        )
        return True


def get_user(guild_id: int, user_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT guild_id, user_id, joined_at, bank, networth, owe, display_name, rank
            FROM users
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
        return None if row is None else dict(row)


def get_users(guild_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT user_id, bank, networth, owe, display_name, rank, joined_at
            FROM users
            WHERE guild_id = ?
            ORDER BY networth DESC, bank DESC, user_id ASC
            """,
            (guild_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def add_commodity(
    guild_id: int,
    name: str,
    price: float,
    rarity: str,
    image_url: str,
    description: str,
) -> bool:
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT 1 FROM commodities WHERE guild_id = ? AND name = ? COLLATE NOCASE",
            (guild_id, name),
        ).fetchone()
        if existing:
            return False
        conn.execute(
            """
            INSERT INTO commodities (guild_id, name, price, rarity, image_url, description)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (guild_id, name, max(0.01, float(price)), rarity, image_url, description),
        )
        return True


def get_commodities(guild_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT name, price, rarity, image_url, description
            FROM commodities
            WHERE guild_id = ?
            ORDER BY name
            """,
            (guild_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def get_commodity(guild_id: int, name: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT name, price, rarity, image_url, description
            FROM commodities
            WHERE guild_id = ? AND name = ? COLLATE NOCASE
            """,
            (guild_id, name),
        ).fetchone()
        return None if row is None else dict(row)


def get_user_commodities(guild_id: int, user_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT uc.commodity_name AS name, uc.quantity, c.price, c.rarity, c.image_url, c.description
            FROM user_commodities uc
            JOIN commodities c
              ON c.guild_id = uc.guild_id
             AND c.name = uc.commodity_name
            WHERE uc.guild_id = ? AND uc.user_id = ? AND uc.quantity > 0
            ORDER BY uc.commodity_name
            """,
            (guild_id, user_id),
        ).fetchall()
        return [dict(row) for row in rows]


def upsert_user_commodity(
    guild_id: int,
    user_id: int,
    commodity_name: str,
    quantity_delta: int,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, commodity_name)
            DO UPDATE SET quantity = quantity + excluded.quantity
            """,
            (guild_id, user_id, commodity_name, quantity_delta),
        )
        conn.execute(
            """
            DELETE FROM user_commodities
            WHERE guild_id = ? AND user_id = ? AND commodity_name = ? AND quantity <= 0
            """,
            (guild_id, user_id, commodity_name),
        )


def update_user_networth(
    guild_id: int,
    user_id: int,
    new_networth: float,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET networth = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (max(0.0, float(new_networth)), guild_id, user_id),
        )


def recalc_user_networth(guild_id: int, user_id: int) -> float:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(uc.quantity * c.price), 0.0) AS total
            FROM user_commodities uc
            JOIN commodities c
              ON c.guild_id = uc.guild_id
             AND c.name = uc.commodity_name
            WHERE uc.guild_id = ? AND uc.user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
        total = float(row["total"]) if row is not None else 0.0
        conn.execute(
            """
            UPDATE users
            SET networth = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (total, guild_id, user_id),
        )
        return total


def recalc_all_networth(guild_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET networth = COALESCE((
                SELECT SUM(uc.quantity * c.price)
                FROM user_commodities uc
                JOIN commodities c
                  ON c.guild_id = uc.guild_id
                 AND c.name = uc.commodity_name
                WHERE uc.guild_id = users.guild_id
                  AND uc.user_id = users.user_id
            ), 0.0)
            WHERE guild_id = ?
            """,
            (guild_id,),
        )


def get_top_users_by_networth(guild_id: int, limit: int = 3) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT user_id, display_name, bank, networth
            FROM users
            WHERE guild_id = ?
            ORDER BY networth DESC, bank DESC, user_id ASC
            LIMIT ?
            """,
            (guild_id, max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]


def add_company(
    guild_id: int,
    symbol: str,
    name: str,
    location: str,
    industry: str,
    founded_year: int,
    description: str,
    evaluation: str,
    base_price: float,
    slope: float,
    drift: float,
    liquidity: float,
    impact_power: float,
    starting_tick: int,
    current_price: float,
    last_tick: int,
    updated_at: str,
) -> bool:
    base_price = max(0.01, float(base_price))
    current_price = max(0.01, float(current_price))
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT 1 FROM companies WHERE guild_id = ? AND symbol = ?",
            (guild_id, symbol),
        ).fetchone()
        if existing:
            return False

        conn.execute(
            """
            INSERT INTO companies (
                guild_id,
                symbol,
                name,
                location,
                industry,
                founded_year,
                description,
                evaluation,
                base_price,
                slope,
                drift,
                liquidity,
                impact_power,
                pending_buy,
                pending_sell,
                starting_tick,
                current_price,
                last_tick,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                symbol,
                name,
                location,
                industry,
                founded_year,
                description,
                evaluation,
                base_price,
                slope,
                drift,
                liquidity,
                impact_power,
                0.0,
                0.0,
                starting_tick,
                current_price,
                last_tick,
                updated_at,
            ),
        )
        return True


def get_companies(guild_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, name, location, industry, founded_year, description, evaluation, base_price, slope, drift, liquidity, impact_power, pending_buy, pending_sell, starting_tick, current_price, last_tick, updated_at
            FROM companies
            WHERE guild_id = ?
            ORDER BY symbol
            """,
            (guild_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def get_company(guild_id: int, symbol: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT symbol, name, location, industry, founded_year, description, evaluation, base_price, slope, drift, liquidity, impact_power, pending_buy, pending_sell, starting_tick, current_price, last_tick, updated_at
            FROM companies
            WHERE guild_id = ? AND symbol = ?
            """,
            (guild_id, symbol),
        ).fetchone()
        return None if row is None else dict(row)


def update_company_slope(
    guild_id: int,
    symbol: str,
    slope: float,
    updated_at: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE companies
            SET slope = ?,
                updated_at = ?
            WHERE guild_id = ? AND symbol = ?
            """,
            (slope, updated_at, guild_id, symbol),
        )


def update_company_price(
    guild_id: int,
    symbol: str,
    base_price: float,
    slope: float,
    drift: float,
    pending_buy: float,
    pending_sell: float,
    current_price: float,
    last_tick: int,
    updated_at: str,
) -> None:
    base_price = max(0.01, float(base_price))
    current_price = max(0.01, float(current_price))
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE companies
            SET base_price = ?,
                slope = ?,
                drift = ?,
                pending_buy = ?,
                pending_sell = ?,
                current_price = ?,
                last_tick = ?,
                updated_at = ?
            WHERE guild_id = ? AND symbol = ?
            """,
            (
                base_price,
                slope,
                drift,
                pending_buy,
                pending_sell,
                current_price,
                last_tick,
                updated_at,
                guild_id,
                symbol,
            ),
        )


def update_company_trade_volume(
    guild_id: int,
    symbol: str,
    buy_delta: float,
    sell_delta: float,
    updated_at: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE companies
            SET pending_buy = pending_buy + ?,
                pending_sell = pending_sell + ?,
                updated_at = ?
            WHERE guild_id = ? AND symbol = ?
            """,
            (buy_delta, sell_delta, updated_at, guild_id, symbol),
        )


def update_user_bank(
    guild_id: int,
    user_id: int,
    new_bank: float,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET bank = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (new_bank, guild_id, user_id),
        )


def update_user_admin_fields(
    guild_id: int,
    user_id: int,
    updates: dict[str, str],
) -> bool:
    allowed = {
        "bank": float,
        "owe": float,
        "rank": str,
    }
    sets: list[str] = []
    values: list[object] = []
    for key, raw in updates.items():
        caster = allowed.get(key)
        if caster is None:
            continue
        value = caster(raw) if caster is not str else str(raw)
        sets.append(f"{key} = ?")
        values.append(value)
    if not sets:
        return False

    with get_connection() as conn:
        cur = conn.execute(
            f"""
            UPDATE users
            SET {", ".join(sets)}
            WHERE guild_id = ? AND user_id = ?
            """,
            (*values, guild_id, user_id),
        )
        return cur.rowcount > 0


def update_company_admin_fields(
    guild_id: int,
    symbol: str,
    updates: dict[str, str],
) -> bool:
    allowed = {
        "name": str,
        "location": str,
        "industry": str,
        "founded_year": int,
        "description": str,
        "evaluation": str,
        "base_price": float,
        "slope": float,
        "drift": float,
        "liquidity": float,
        "impact_power": float,
        "current_price": float,
    }
    sets: list[str] = []
    values: list[object] = []
    for key, raw in updates.items():
        caster = allowed.get(key)
        if caster is None:
            continue
        value = caster(raw) if caster is not str else str(raw)
        sets.append(f"{key} = ?")
        values.append(value)
    if not sets:
        return False

    with get_connection() as conn:
        cur = conn.execute(
            f"""
            UPDATE companies
            SET {", ".join(sets)}
            WHERE guild_id = ? AND symbol = ?
            """,
            (*values, guild_id, symbol),
        )
        return cur.rowcount > 0


def update_commodity_admin_fields(
    guild_id: int,
    name: str,
    updates: dict[str, str],
) -> bool:
    allowed = {
        "name": str,
        "price": float,
        "rarity": str,
        "image_url": str,
        "description": str,
    }
    sets: list[str] = []
    values: list[object] = []
    for key, raw in updates.items():
        caster = allowed.get(key)
        if caster is None:
            continue
        value = caster(raw) if caster is not str else str(raw)
        sets.append(f"{key} = ?")
        values.append(value)
    if not sets:
        return False

    with get_connection() as conn:
        cur = conn.execute(
            f"""
            UPDATE commodities
            SET {", ".join(sets)}
            WHERE guild_id = ? AND name = ? COLLATE NOCASE
            """,
            (*values, guild_id, name),
        )
        if cur.rowcount <= 0:
            return False
        return True


def backfill_company_starting_ticks() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE companies
            SET starting_tick = (
                SELECT COALESCE(MIN(ph.tick_index), 0)
                FROM price_history ph
                WHERE ph.guild_id = companies.guild_id
                  AND ph.symbol = companies.symbol
            )
            WHERE starting_tick = 0
            """
        )
        conn.execute(
            """
            UPDATE companies
            SET last_tick = (
                SELECT COALESCE(MAX(ph.tick_index), 0)
                FROM price_history ph
                WHERE ph.guild_id = companies.guild_id
                  AND ph.symbol = companies.symbol
            )
            WHERE last_tick = 0
            """
        )


def add_price_history(
    guild_id: int,
    symbol: str,
    tick_index: int,
    ts: str,
    price: float,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO price_history (guild_id, symbol, tick_index, ts, price)
            VALUES (?, ?, ?, ?, ?)
            """,
            (guild_id, symbol, tick_index, ts, price),
        )


def get_price_history(
    guild_id: int,
    symbol: str,
    limit: int | None = 50,
) -> list[dict]:
    with get_connection() as conn:
        if limit is None:
            rows = conn.execute(
                """
                SELECT ts, price
                FROM price_history
                WHERE guild_id = ? AND symbol = ?
                ORDER BY tick_index DESC
                """,
                (guild_id, symbol),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ts, price
                FROM price_history
                WHERE guild_id = ? AND symbol = ?
                ORDER BY tick_index DESC
                LIMIT ?
                """,
                (guild_id, symbol, limit),
            ).fetchall()

        return [dict(row) for row in rows[::-1]]


def get_state_value(key: str) -> str | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM app_state WHERE key = ?",
            (key,),
        ).fetchone()
        return None if row is None else row["value"]


def set_state_value(key: str, value: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def clear_state_prefix(prefix: str) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            DELETE FROM app_state
            WHERE key LIKE ?
            """,
            (f"{prefix}%",),
        )
        return int(cur.rowcount)


def get_user_status(guild_id: int, user_id: int) -> dict | None:
    with get_connection() as conn:
        user = conn.execute(
            """
            SELECT guild_id, user_id, joined_at, bank, networth, owe, display_name, rank
            FROM users
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
        if user is None:
            return None

        holdings = conn.execute(
            """
            SELECT symbol, shares
            FROM holdings
            WHERE guild_id = ? AND user_id = ?
            ORDER BY symbol
            """,
            (guild_id, user_id),
        ).fetchall()

        return {
            "user": dict(user),
            "holdings": [dict(row) for row in holdings],
        }


def get_user_shares(
    guild_id: int,
    user_id: int,
    symbol: str,
) -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT shares
            FROM holdings
            WHERE guild_id = ? AND user_id = ? AND symbol = ?
            """,
            (guild_id, user_id, symbol),
        ).fetchone()
        return 0 if row is None else int(row["shares"])


def upsert_holding(
    guild_id: int,
    user_id: int,
    symbol: str,
    shares_delta: int,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO holdings (guild_id, user_id, symbol, shares)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, symbol)
            DO UPDATE SET shares = shares + excluded.shares
            """,
            (guild_id, user_id, symbol, shares_delta),
        )


def set_holding(
    guild_id: int,
    user_id: int,
    symbol: str,
    shares: int,
) -> None:
    with get_connection() as conn:
        if shares <= 0:
            conn.execute(
                """
                DELETE FROM holdings
                WHERE guild_id = ? AND user_id = ? AND symbol = ?
                """,
                (guild_id, user_id, symbol),
            )
            return

        conn.execute(
            """
            INSERT INTO holdings (guild_id, user_id, symbol, shares)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, symbol)
            DO UPDATE SET shares = excluded.shares
            """,
            (guild_id, user_id, symbol, shares),
        )


def add_action_history(
    guild_id: int,
    user_id: int,
    action_type: str,
    target_type: str,
    target_symbol: str,
    quantity: float,
    unit_price: float,
    total_amount: float,
    details: str,
    created_at: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO action_history (
                guild_id,
                user_id,
                action_type,
                target_type,
                target_symbol,
                quantity,
                unit_price,
                total_amount,
                details,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                user_id,
                action_type,
                target_type,
                target_symbol,
                float(quantity),
                float(unit_price),
                float(total_amount),
                details,
                created_at,
            ),
        )


def get_action_history(
    guild_id: int,
    limit: int = 300,
) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                ah.id,
                ah.guild_id,
                ah.user_id,
                COALESCE(u.display_name, '') AS display_name,
                ah.action_type,
                ah.target_type,
                ah.target_symbol,
                ah.quantity,
                ah.unit_price,
                ah.total_amount,
                ah.details,
                ah.created_at
            FROM action_history ah
            LEFT JOIN users u
              ON u.guild_id = ah.guild_id
             AND u.user_id = ah.user_id
            WHERE ah.guild_id = ?
            ORDER BY ah.id DESC
            LIMIT ?
            """,
            (guild_id, max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]


def create_bank_request(
    guild_id: int,
    user_id: int,
    request_type: str,
    amount: float,
    reason: str,
    created_at: str,
) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO bank_requests (
                guild_id,
                user_id,
                request_type,
                amount,
                reason,
                status,
                decision_reason,
                reviewed_by,
                created_at,
                reviewed_at,
                processed_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', '', 0, ?, '', '')
            """,
            (
                guild_id,
                user_id,
                request_type,
                max(0.0, float(amount)),
                reason,
                created_at,
            ),
        )
        return int(cur.lastrowid)


def add_feedback(
    guild_id: int,
    message: str,
    created_at: str,
) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO feedback (
                guild_id,
                message,
                created_at
            )
            VALUES (?, ?, ?)
            """,
            (
                guild_id,
                message,
                created_at,
            ),
        )
        return int(cur.lastrowid)


def get_feedback(
    guild_id: int | None = None,
    limit: int = 500,
) -> list[dict]:
    where = ""
    values: list[object] = []
    if guild_id is not None:
        where = "WHERE guild_id = ?"
        values.append(int(guild_id))
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, guild_id, message, created_at
            FROM feedback
            {where}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*values, max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]


def get_close_news(
    guild_id: int,
    *,
    enabled_only: bool = False,
) -> list[dict]:
    with get_connection() as conn:
        if enabled_only:
            rows = conn.execute(
                """
                SELECT id, guild_id, title, body, image_url, sort_order, enabled, updated_at
                FROM close_news
                WHERE guild_id = ? AND enabled = 1
                ORDER BY sort_order ASC, id ASC
                """,
                (guild_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, guild_id, title, body, image_url, sort_order, enabled, updated_at
                FROM close_news
                WHERE guild_id = ?
                ORDER BY sort_order ASC, id ASC
                """,
                (guild_id,),
            ).fetchall()
        return [dict(row) for row in rows]


def get_bank_requests(
    *,
    guild_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[dict]:
    where_parts: list[str] = []
    values: list[object] = []
    if guild_id is not None:
        where_parts.append("br.guild_id = ?")
        values.append(int(guild_id))
    if status is not None:
        where_parts.append("br.status = ?")
        values.append(str(status))

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                br.id,
                br.guild_id,
                br.user_id,
                COALESCE(u.display_name, '') AS display_name,
                br.request_type,
                br.amount,
                br.reason,
                br.status,
                br.decision_reason,
                br.reviewed_by,
                br.created_at,
                br.reviewed_at,
                br.processed_at
            FROM bank_requests br
            LEFT JOIN users u
              ON u.guild_id = br.guild_id
             AND u.user_id = br.user_id
            {where_clause}
            ORDER BY br.id DESC
            LIMIT ?
            """,
            (*values, max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]


def set_bank_request_decision(
    *,
    request_id: int,
    status: str,
    reviewed_by: int,
    decision_reason: str,
    reviewed_at: str,
) -> bool:
    if status not in {"approved", "denied"}:
        return False

    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE bank_requests
            SET status = ?,
                decision_reason = ?,
                reviewed_by = ?,
                reviewed_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (
                status,
                decision_reason,
                int(reviewed_by),
                reviewed_at,
                int(request_id),
            ),
        )
        return cur.rowcount > 0


def get_unprocessed_reviewed_bank_requests(
    *,
    guild_id: int | None = None,
    limit: int = 200,
) -> list[dict]:
    where_parts = ["status IN ('approved', 'denied')", "processed_at = ''"]
    values: list[object] = []
    if guild_id is not None:
        where_parts.append("guild_id = ?")
        values.append(int(guild_id))

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, guild_id, user_id, request_type, amount, reason, status, decision_reason, reviewed_by, created_at, reviewed_at, processed_at
            FROM bank_requests
            WHERE {" AND ".join(where_parts)}
            ORDER BY id ASC
            LIMIT ?
            """,
            (*values, max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]


def mark_bank_request_processed(request_id: int, processed_at: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE bank_requests
            SET processed_at = ?
            WHERE id = ?
            """,
            (processed_at, int(request_id)),
        )


def apply_approved_loan(
    *,
    guild_id: int,
    user_id: int,
    amount: float,
) -> bool:
    delta = max(0.0, float(amount))
    if delta <= 0:
        return False
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT bank, owe
            FROM users
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
        if row is None:
            return False
        bank = float(row["bank"])
        owe = float(row["owe"])
        conn.execute(
            """
            UPDATE users
            SET bank = ?,
                owe = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (bank + delta, owe + delta, guild_id, user_id),
        )
        return True


def repay_user_loan(
    *,
    guild_id: int,
    user_id: int,
    amount: float,
) -> dict | None:
    requested = max(0.0, float(amount))
    if requested <= 0:
        return None

    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT bank, owe
            FROM users
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
        if row is None:
            return None

        bank = float(row["bank"])
        owe = float(row["owe"])
        paid = min(requested, bank, owe)
        if paid <= 0:
            return {
                "paid": 0.0,
                "bank_after": bank,
                "owe_after": owe,
            }

        bank_after = bank - paid
        owe_after = owe - paid
        conn.execute(
            """
            UPDATE users
            SET bank = ?,
                owe = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (bank_after, owe_after, guild_id, user_id),
        )
        return {
            "paid": paid,
            "bank_after": bank_after,
            "owe_after": owe_after,
        }


def create_database_backup(
    *,
    prefix: str = "stockbot",
) -> str:
    db_path = Path(DB_PATH)
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    backup_path = backup_dir / f"{prefix}_{stamp}.db"

    with get_connection() as source_conn, sqlite3.connect(backup_path) as backup_conn:
        source_conn.backup(backup_conn)

    return str(backup_path)


def purge_all_data() -> None:
    db_path = Path(DB_PATH)

    # Ensure no open sqlite handle from this process before deleting the file.
    with get_connection():
        pass

    if db_path.exists():
        db_path.unlink()

    # Recreate schema immediately so the bot can continue without restart.
    init_db()


def wipe_price_history(guild_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM price_history
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM daily_close
            WHERE guild_id = ?
            """,
            (guild_id,),
        )


def wipe_users(guild_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM users
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM user_commodities
            WHERE guild_id = ?
            """,
            (guild_id,),
        )


def wipe_companies(guild_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM companies
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM price_history
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM daily_close
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM holdings
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM commodities
            WHERE guild_id = ?
            """,
            (guild_id,),
        )
        conn.execute(
            """
            DELETE FROM user_commodities
            WHERE guild_id = ?
            """,
            (guild_id,),
        )


def upsert_daily_close(
    guild_id: int,
    symbol: str,
    date: str,
    close_price: float,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO daily_close (guild_id, symbol, date, close_price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, symbol, date)
            DO UPDATE SET close_price = excluded.close_price
            """,
            (guild_id, symbol, date, close_price),
        )


def get_latest_close(
    guild_id: int,
    symbol: str,
) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT date, close_price
            FROM daily_close
            WHERE guild_id = ? AND symbol = ?
            ORDER BY date DESC
            LIMIT 1
            """,
            (guild_id, symbol),
        ).fetchone()
        return None if row is None else dict(row)
