from stockbot.db.database import get_connection


def register_user(
    guild_id: int,
    user_id: int,
    joined_at: str,
    bank: int,
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
            INSERT INTO users (guild_id, user_id, joined_at, bank, rank)
            VALUES (?, ?, ?, ?, ?)
            """,
            (guild_id, user_id, joined_at, bank, rank),
        )
        return True


def get_user(guild_id: int, user_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT guild_id, user_id, joined_at, bank, rank
            FROM users
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()
        return None if row is None else dict(row)


def add_company(
    guild_id: int,
    symbol: str,
    name: str,
    base_price: float,
    slope: float,
    drift: float,
    player_impact: float,
    starting_tick: int,
    current_price: float,
    last_tick: int,
    updated_at: str,
) -> bool:
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
                base_price,
                slope,
                drift,
                player_impact,
                starting_tick,
                current_price,
                last_tick,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                symbol,
                name,
                base_price,
                slope,
                drift,
                player_impact,
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
            SELECT symbol, name, base_price, slope, drift, player_impact, starting_tick, current_price, last_tick, updated_at
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
            SELECT symbol, name, base_price, slope, drift, player_impact, starting_tick, current_price, last_tick, updated_at
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
    current_price: float,
    last_tick: int,
    updated_at: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE companies
            SET base_price = ?,
                slope = ?,
                drift = ?,
                current_price = ?,
                last_tick = ?,
                updated_at = ?
            WHERE guild_id = ? AND symbol = ?
            """,
            (
                base_price,
                slope,
                drift,
                current_price,
                last_tick,
                updated_at,
                guild_id,
                symbol,
            ),
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


def get_user_status(guild_id: int, user_id: int) -> dict | None:
    with get_connection() as conn:
        user = conn.execute(
            """
            SELECT guild_id, user_id, joined_at, bank, rank
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


def purge_all_data() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            DELETE FROM daily_close;
            DELETE FROM price_history;
            DELETE FROM holdings;
            DELETE FROM users;
            DELETE FROM companies;
            DELETE FROM app_state;
            """
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
