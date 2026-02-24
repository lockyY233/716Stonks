from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from stockbot.config import DB_PATH
from stockbot.db.database import get_connection, init_db
from stockbot.services.money import money


def _parse_owner_user_ids(raw: str | int | list[int] | tuple[int, ...] | None) -> list[int]:
    if raw is None:
        return []
    if isinstance(raw, int):
        return [raw] if raw > 0 else []
    if isinstance(raw, (list, tuple)):
        seen: set[int] = set()
        out: list[int] = []
        for item in raw:
            try:
                uid = int(item)
            except (TypeError, ValueError):
                continue
            if uid <= 0 or uid in seen:
                continue
            seen.add(uid)
            out.append(uid)
        return out
    text = str(raw).strip()
    if not text:
        return []
    text = text.replace(";", ",").replace("|", ",")
    tokens = [t.strip() for t in text.split(",")]
    seen: set[int] = set()
    out: list[int] = []
    for token in tokens:
        if not token:
            continue
        if token.startswith("+"):
            token = token[1:]
        if not token.isdigit():
            continue
        uid = int(token)
        if uid <= 0 or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _set_company_owners(
    conn: sqlite3.Connection,
    guild_id: int,
    symbol: str,
    owner_user_ids: list[int],
) -> None:
    conn.execute(
        """
        DELETE FROM company_owners
        WHERE guild_id = ? AND symbol = ?
        """,
        (guild_id, symbol),
    )
    for uid in owner_user_ids:
        conn.execute(
            """
            INSERT OR IGNORE INTO company_owners (guild_id, symbol, user_id)
            VALUES (?, ?, ?)
            """,
            (guild_id, symbol, uid),
        )


def _hydrate_company_owner_ids(
    conn: sqlite3.Connection,
    guild_id: int,
    companies: list[dict],
) -> None:
    if not companies:
        return
    symbols = [str(c.get("symbol", "")) for c in companies if str(c.get("symbol", ""))]
    if not symbols:
        return
    placeholders = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"""
        SELECT symbol, user_id
        FROM company_owners
        WHERE guild_id = ? AND symbol IN ({placeholders})
        ORDER BY symbol, user_id
        """,
        (guild_id, *symbols),
    ).fetchall()
    owners_map: dict[str, list[int]] = {}
    for row in rows:
        sym = str(row["symbol"])
        owners_map.setdefault(sym, []).append(int(row["user_id"]))
    for company in companies:
        symbol = str(company.get("symbol", ""))
        owner_ids = owners_map.get(symbol, [])
        if not owner_ids:
            legacy_owner = int(company.get("owner_user_id", 0) or 0)
            owner_ids = [legacy_owner] if legacy_owner > 0 else []
        company["owner_user_ids"] = owner_ids
        company["owner_user_id"] = owner_ids[0] if owner_ids else 0


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


def get_jobs(guild_id: int, enabled_only: bool = False) -> list[dict]:
    with get_connection() as conn:
        if enabled_only:
            rows = conn.execute(
                """
                SELECT id, guild_id, name, description, job_type, enabled, cooldown_ticks,
                       reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                       quiz_question, quiz_answer, config_json, updated_at
                FROM jobs
                WHERE guild_id = ? AND enabled = 1
                ORDER BY id ASC
                """,
                (guild_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, guild_id, name, description, job_type, enabled, cooldown_ticks,
                       reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                       quiz_question, quiz_answer, config_json, updated_at
                FROM jobs
                WHERE guild_id = ?
                ORDER BY id ASC
                """,
                (guild_id,),
            ).fetchall()
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            duration_minutes = float(item.get("duration_minutes", 0.0) or 0.0)
            if duration_minutes <= 0.0:
                duration_minutes = float(item.get("duration_ticks", 0) or 0)
            item["duration_minutes"] = duration_minutes
            out.append(item)
        return out


def get_job(guild_id: int, job_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, guild_id, name, description, job_type, enabled, cooldown_ticks,
                   reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                   quiz_question, quiz_answer, config_json, updated_at
            FROM jobs
            WHERE guild_id = ? AND id = ?
            """,
            (guild_id, job_id),
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        duration_minutes = float(item.get("duration_minutes", 0.0) or 0.0)
        if duration_minutes <= 0.0:
            duration_minutes = float(item.get("duration_ticks", 0) or 0)
        item["duration_minutes"] = duration_minutes
        return item


def create_job(
    guild_id: int,
    *,
    name: str,
    description: str,
    job_type: str,
    enabled: int,
    cooldown_ticks: int,
    reward_min: float,
    reward_max: float,
    success_rate: float,
    duration_minutes: float,
    quiz_question: str,
    quiz_answer: str,
    config_json: str,
    updated_at: str,
) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO jobs (
                guild_id, name, description, job_type, enabled, cooldown_ticks,
                reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                quiz_question, quiz_answer, config_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                name,
                description,
                job_type,
                int(enabled),
                int(cooldown_ticks),
                float(reward_min),
                float(reward_max),
                float(success_rate),
                max(0, int(float(duration_minutes))),
                max(0.0, float(duration_minutes)),
                quiz_question,
                quiz_answer,
                config_json,
                updated_at,
            ),
        )
        return int(cur.lastrowid)


def update_job(guild_id: int, job_id: int, updates: dict[str, object]) -> bool:
    if not updates:
        return False
    sets = []
    values: list[object] = []
    for k, v in updates.items():
        sets.append(f"{k} = ?")
        values.append(v)
    values.extend([guild_id, job_id])
    with get_connection() as conn:
        cur = conn.execute(
            f"""
            UPDATE jobs
            SET {", ".join(sets)}
            WHERE guild_id = ? AND id = ?
            """,
            values,
        )
        return cur.rowcount > 0


def delete_job(guild_id: int, job_id: int) -> bool:
    with get_connection() as conn:
        cur = conn.execute(
            """
            DELETE FROM jobs
            WHERE guild_id = ? AND id = ?
            """,
            (guild_id, job_id),
        )
        return cur.rowcount > 0


def get_user_job_state(guild_id: int, user_id: int, job_id: int) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT guild_id, user_id, job_id, next_available_tick, running_until_tick, running_until_epoch
            FROM user_jobs
            WHERE guild_id = ? AND user_id = ? AND job_id = ?
            """,
            (guild_id, user_id, job_id),
        ).fetchone()
        if row is None:
            return {
                "guild_id": guild_id,
                "user_id": user_id,
                "job_id": job_id,
                "next_available_tick": 0,
                "running_until_tick": 0,
                "running_until_epoch": 0.0,
            }
        item = dict(row)
        item["running_until_epoch"] = float(item.get("running_until_epoch", 0.0) or 0.0)
        return item


def set_user_job_state(
    guild_id: int,
    user_id: int,
    job_id: int,
    *,
    next_available_tick: int,
    running_until_tick: int,
    running_until_epoch: float = 0.0,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO user_jobs (
                guild_id, user_id, job_id, next_available_tick, running_until_tick, running_until_epoch
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, job_id)
            DO UPDATE SET
                next_available_tick = excluded.next_available_tick,
                running_until_tick = excluded.running_until_tick,
                running_until_epoch = excluded.running_until_epoch
            """,
            (
                guild_id,
                user_id,
                job_id,
                int(next_available_tick),
                int(running_until_tick),
                max(0.0, float(running_until_epoch)),
            ),
        )


def get_user_running_timed_job(guild_id: int, user_id: int, now_tick: int = 0) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                uj.guild_id,
                uj.user_id,
                uj.job_id,
                uj.running_until_tick,
                uj.running_until_epoch,
                j.name AS job_name
            FROM user_jobs uj
            JOIN jobs j
              ON j.id = uj.job_id
             AND j.guild_id = uj.guild_id
            WHERE uj.guild_id = ?
              AND uj.user_id = ?
              AND j.job_type = 'timed'
              AND (uj.running_until_epoch > 0 OR uj.running_until_tick > ?)
            ORDER BY
              CASE
                WHEN uj.running_until_epoch > 0 THEN uj.running_until_epoch
                ELSE CAST(uj.running_until_tick AS REAL)
              END DESC
            LIMIT 1
            """,
            (guild_id, user_id, int(now_tick)),
        ).fetchone()
        return None if row is None else dict(row)


def list_running_timed_jobs(guild_id: int | None = None, limit: int = 500) -> list[dict]:
    query = """
        SELECT
            uj.guild_id,
            uj.user_id,
            uj.job_id,
            uj.running_until_tick,
            uj.running_until_epoch,
            j.name AS job_name
        FROM user_jobs uj
        JOIN jobs j
          ON j.id = uj.job_id
         AND j.guild_id = uj.guild_id
        WHERE j.job_type = 'timed'
          AND j.enabled = 1
          AND (uj.running_until_epoch > 0 OR uj.running_until_tick > 0)
    """
    params: list[object] = []
    if guild_id is not None:
        query += " AND uj.guild_id = ?"
        params.append(int(guild_id))
    query += """
        ORDER BY
          uj.guild_id ASC,
          CASE
            WHEN uj.running_until_epoch > 0 THEN uj.running_until_epoch
            ELSE CAST(uj.running_until_tick AS REAL)
          END ASC,
          uj.user_id ASC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(r) for r in rows]


def count_user_running_timed_jobs(guild_id: int, user_id: int, now_tick: int) -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM user_jobs uj
            JOIN jobs j
              ON j.id = uj.job_id
             AND j.guild_id = uj.guild_id
            WHERE uj.guild_id = ?
              AND uj.user_id = ?
              AND j.job_type = 'timed'
              AND j.enabled = 1
              AND (uj.running_until_epoch > strftime('%s','now') OR uj.running_until_tick > ?)
            """,
            (guild_id, user_id, int(now_tick)),
        ).fetchone()
        return int(row["cnt"] or 0) if row is not None else 0


def add_commodity(
    guild_id: int,
    name: str,
    price: float,
    rarity: str,
    image_url: str,
    description: str,
    enabled: int = 1,
    spawn_weight_override: float = 0.0,
    perk_name: str = "",
    perk_description: str = "",
    perk_min_qty: int = 1,
    perk_effects_json: str = "",
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
            INSERT INTO commodities (
                guild_id, name, price, enabled, rarity, spawn_weight_override, image_url, description,
                perk_name, perk_description, perk_min_qty, perk_effects_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                name,
                max(0.01, float(price)),
                1 if int(enabled) else 0,
                rarity,
                max(0.0, float(spawn_weight_override)),
                image_url,
                description,
                str(perk_name or ""),
                str(perk_description or ""),
                max(1, int(perk_min_qty)),
                str(perk_effects_json or ""),
            ),
        )
        return True


def get_commodities(guild_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT name, price, enabled, rarity, spawn_weight_override, image_url, description,
                   perk_name, perk_description, perk_min_qty, perk_effects_json
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
            SELECT name, price, enabled, rarity, spawn_weight_override, image_url, description,
                   perk_name, perk_description, perk_min_qty, perk_effects_json
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
            SELECT
                c.name AS name,
                CAST(SUM(uc.quantity) AS INTEGER) AS quantity,
                c.price,
                c.rarity,
                c.spawn_weight_override,
                c.image_url,
                c.description
            FROM user_commodities uc
            JOIN commodities c
              ON c.guild_id = uc.guild_id
             AND c.name = uc.commodity_name COLLATE NOCASE
            WHERE uc.guild_id = ? AND uc.user_id = ? AND uc.quantity > 0
            GROUP BY
                c.name, c.price, c.rarity, c.spawn_weight_override, c.image_url, c.description
            ORDER BY c.name
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
        canonical_row = conn.execute(
            """
            SELECT name
            FROM commodities
            WHERE guild_id = ? AND name = ? COLLATE NOCASE
            LIMIT 1
            """,
            (guild_id, commodity_name),
        ).fetchone()
        canonical_name = str(canonical_row["name"]) if canonical_row is not None else str(commodity_name)
        conn.execute(
            """
            INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, commodity_name)
            DO UPDATE SET quantity = quantity + excluded.quantity
            """,
            (guild_id, user_id, canonical_name, quantity_delta),
        )
        conn.execute(
            """
            DELETE FROM user_commodities
            WHERE guild_id = ? AND user_id = ? AND commodity_name = ? COLLATE NOCASE AND quantity <= 0
            """,
            (guild_id, user_id, canonical_name),
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
            (max(0.0, money(new_networth)), guild_id, user_id),
        )


def recalc_user_networth(guild_id: int, user_id: int) -> float:
    with get_connection() as conn:
        row = conn.execute(
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
        total = float(row["total"]) if row is not None else 0.0
        conn.execute(
            """
            UPDATE users
            SET networth = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (money(total), guild_id, user_id),
        )
        return money(total)


def recalc_all_networth(guild_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET networth = ROUND(COALESCE((
                SELECT SUM(uc.quantity * c.price)
                FROM user_commodities uc
                JOIN commodities c
                  ON c.guild_id = uc.guild_id
                 AND c.name = uc.commodity_name COLLATE NOCASE
                WHERE uc.guild_id = users.guild_id
                  AND uc.user_id = users.user_id
            ), 0.0), 2)
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
    owner_user_id: int,
    owner_user_ids: list[int] | None,
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
    parsed_owner_ids = _parse_owner_user_ids(owner_user_ids)
    if not parsed_owner_ids:
        parsed_owner_ids = _parse_owner_user_ids(owner_user_id)
    canonical_owner = parsed_owner_ids[0] if parsed_owner_ids else 0
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
                owner_user_id,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                symbol,
                name,
                canonical_owner,
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
        _set_company_owners(conn, guild_id, symbol, parsed_owner_ids)
        return True


def get_companies(guild_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT symbol, name, owner_user_id, location, industry, founded_year, description, evaluation, base_price, slope, drift, liquidity, impact_power, pending_buy, pending_sell, starting_tick, current_price, last_tick, updated_at
            FROM companies
            WHERE guild_id = ?
            ORDER BY symbol
            """,
            (guild_id,),
        ).fetchall()
        companies = [dict(row) for row in rows]
        _hydrate_company_owner_ids(conn, guild_id, companies)
        return companies


def get_company(guild_id: int, symbol: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT symbol, name, owner_user_id, location, industry, founded_year, description, evaluation, base_price, slope, drift, liquidity, impact_power, pending_buy, pending_sell, starting_tick, current_price, last_tick, updated_at
            FROM companies
            WHERE guild_id = ? AND symbol = ?
            """,
            (guild_id, symbol),
        ).fetchone()
        if row is None:
            return None
        company = dict(row)
        _hydrate_company_owner_ids(conn, guild_id, [company])
        return company


def create_stock_alert(
    guild_id: int,
    user_id: int,
    symbol: str,
    condition: str,
    target_price: float,
    repeat_enabled: bool = False,
) -> int:
    cond = str(condition).strip().lower()
    if cond not in {"above", "below"}:
        raise ValueError("condition must be 'above' or 'below'")
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO stock_alerts (
                guild_id, user_id, symbol, condition, target_price,
                repeat_enabled, enabled, last_met, last_triggered_tick,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, 0, 0, ?, ?)
            """,
            (
                int(guild_id),
                int(user_id),
                str(symbol).upper().strip(),
                cond,
                max(0.01, float(target_price)),
                1 if repeat_enabled else 0,
                now,
                now,
            ),
        )
        return int(cur.lastrowid)


def get_stock_alerts_for_user(guild_id: int, user_id: int, include_disabled: bool = False) -> list[dict]:
    with get_connection() as conn:
        if include_disabled:
            rows = conn.execute(
                """
                SELECT id, guild_id, user_id, symbol, condition, target_price,
                       repeat_enabled, enabled, last_met, last_triggered_tick, created_at, updated_at
                FROM stock_alerts
                WHERE guild_id = ? AND user_id = ?
                ORDER BY id DESC
                """,
                (int(guild_id), int(user_id)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, guild_id, user_id, symbol, condition, target_price,
                       repeat_enabled, enabled, last_met, last_triggered_tick, created_at, updated_at
                FROM stock_alerts
                WHERE guild_id = ? AND user_id = ? AND enabled = 1
                ORDER BY id DESC
                """,
                (int(guild_id), int(user_id)),
            ).fetchall()
        return [dict(r) for r in rows]


def delete_stock_alert(guild_id: int, user_id: int, alert_id: int) -> bool:
    with get_connection() as conn:
        cur = conn.execute(
            """
            DELETE FROM stock_alerts
            WHERE guild_id = ? AND user_id = ? AND id = ?
            """,
            (int(guild_id), int(user_id), int(alert_id)),
        )
        return cur.rowcount > 0


def clear_stock_alerts_for_user(guild_id: int, user_id: int) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            DELETE FROM stock_alerts
            WHERE guild_id = ? AND user_id = ?
            """,
            (int(guild_id), int(user_id)),
        )
        return int(cur.rowcount)


def process_stock_alerts(guild_id: int, tick_index: int) -> list[dict]:
    now_iso = datetime.now(timezone.utc).isoformat()
    fired: list[dict] = []
    with get_connection() as conn:
        alerts = conn.execute(
            """
            SELECT id, guild_id, user_id, symbol, condition, target_price,
                   repeat_enabled, enabled, last_met, last_triggered_tick
            FROM stock_alerts
            WHERE guild_id = ? AND enabled = 1
            """,
            (int(guild_id),),
        ).fetchall()
        if not alerts:
            return fired
        company_rows = conn.execute(
            """
            SELECT symbol, current_price
            FROM companies
            WHERE guild_id = ?
            """,
            (int(guild_id),),
        ).fetchall()
        price_by_symbol = {
            str(r["symbol"]).upper(): float(r["current_price"])
            for r in company_rows
        }
        for alert in alerts:
            symbol = str(alert["symbol"]).upper()
            price = price_by_symbol.get(symbol)
            if price is None:
                continue
            cond = str(alert["condition"]).lower()
            target = float(alert["target_price"])
            met = price >= target if cond == "above" else price <= target
            last_met = int(alert["last_met"] or 0) > 0
            repeat_enabled = int(alert["repeat_enabled"] or 0) > 0
            should_fire = False
            if met:
                if repeat_enabled:
                    should_fire = not last_met
                else:
                    should_fire = True
            if should_fire:
                fired.append(
                    {
                        "id": int(alert["id"]),
                        "user_id": int(alert["user_id"]),
                        "symbol": symbol,
                        "condition": cond,
                        "target_price": target,
                        "current_price": float(price),
                    }
                )
            enabled_next = 1
            if should_fire and not repeat_enabled:
                enabled_next = 0
            conn.execute(
                """
                UPDATE stock_alerts
                SET enabled = ?,
                    last_met = ?,
                    last_triggered_tick = ?,
                    updated_at = ?
                WHERE id = ? AND guild_id = ?
                """,
                (
                    enabled_next,
                    1 if met else 0,
                    int(tick_index) if should_fire else int(alert["last_triggered_tick"] or 0),
                    now_iso,
                    int(alert["id"]),
                    int(guild_id),
                ),
            )
    return fired


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
            (money(new_bank), guild_id, user_id),
        )


def apply_user_bank_delta_with_loan_floor(
    guild_id: int,
    user_id: int,
    delta: float,
) -> dict:
    """
    Apply a bank delta while ensuring bank never drops below 0.
    Any negative overflow is moved into `owe`.
    """
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
            return {
                "ok": False,
                "bank_before": 0.0,
                "owe_before": 0.0,
                "bank_after": 0.0,
                "owe_after": 0.0,
                "delta": float(delta),
                "to_owe": 0.0,
            }

        bank_before = float(row["bank"])
        owe_before = float(row["owe"])
        target_bank = bank_before + float(delta)
        to_owe = 0.0
        if target_bank < 0:
            to_owe = -target_bank
            bank_after = 0.0
            owe_after = owe_before + to_owe
        else:
            bank_after = target_bank
            owe_after = owe_before

        conn.execute(
            """
            UPDATE users
            SET bank = ?, owe = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (money(bank_after), money(owe_after), guild_id, user_id),
        )
        return {
            "ok": True,
            "bank_before": money(bank_before),
            "owe_before": money(owe_before),
            "bank_after": money(bank_after),
            "owe_after": money(owe_after),
            "delta": float(delta),
            "to_owe": money(to_owe),
        }


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
        if key == "enabled":
            value = 1 if int(value) else 0
        if key in {"bank", "owe"}:
            value = money(float(value))
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
        "owner_user_id": int,
        "owner_user_ids": str,
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
        if key == "owner_user_ids":
            continue
        value = caster(raw) if caster is not str else str(raw)
        sets.append(f"{key} = ?")
        values.append(value)
    if not sets:
        if "owner_user_ids" not in updates:
            return False

    owner_ids = _parse_owner_user_ids(updates.get("owner_user_ids"))
    if not owner_ids and "owner_user_id" in updates:
        owner_ids = _parse_owner_user_ids(updates.get("owner_user_id"))
    if owner_ids:
        # Ensure legacy owner column stays mirrored to first owner.
        owner_idx = next((i for i, field in enumerate(sets) if field == "owner_user_id = ?"), None)
        if owner_idx is None:
            sets.append("owner_user_id = ?")
            values.append(owner_ids[0])
        else:
            values[owner_idx] = owner_ids[0]
    elif "owner_user_ids" in updates:
        # Explicitly clearing owners.
        owner_idx = next((i for i, field in enumerate(sets) if field == "owner_user_id = ?"), None)
        if owner_idx is None:
            sets.append("owner_user_id = ?")
            values.append(0)
        else:
            values[owner_idx] = 0

    with get_connection() as conn:
        cur = conn.execute(
            f"""
            UPDATE companies
            SET {", ".join(sets)}
            WHERE guild_id = ? AND symbol = ?
            """,
            (*values, guild_id, symbol),
        )
        if "owner_user_ids" in updates or "owner_user_id" in updates:
            _set_company_owners(conn, guild_id, symbol, owner_ids)
        return cur.rowcount > 0


def update_commodity_admin_fields(
    guild_id: int,
    name: str,
    updates: dict[str, str],
) -> bool:
    allowed = {
        "name": str,
        "price": float,
        "enabled": int,
        "rarity": str,
        "spawn_weight_override": float,
        "image_url": str,
        "description": str,
        "perk_name": str,
        "perk_description": str,
        "perk_min_qty": int,
        "perk_effects_json": str,
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
            (money(bank + delta), money(owe + delta), guild_id, user_id),
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
                "bank_after": money(bank),
                "owe_after": money(owe),
            }

        bank_after = money(bank - paid)
        owe_after = money(owe - paid)
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
            "paid": money(paid),
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
