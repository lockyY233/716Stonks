import sqlite3
from pathlib import Path

from stockbot.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                joined_at TEXT NOT NULL,
                bank REAL NOT NULL,
                rank TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS holdings (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                shares INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id, symbol),
                FOREIGN KEY (guild_id, user_id)
                    REFERENCES users (guild_id, user_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS companies (
                guild_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                base_price REAL NOT NULL,
                slope REAL NOT NULL,
                drift REAL NOT NULL,
                player_impact REAL NOT NULL DEFAULT 0.5,
                starting_tick INTEGER NOT NULL,
                current_price REAL NOT NULL DEFAULT 0,
                last_tick INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, symbol)
            );

            CREATE TABLE IF NOT EXISTS price_history (
                guild_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                tick_index INTEGER NOT NULL,
                ts TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (guild_id, symbol, tick_index)
            );

            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_close (
                guild_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                close_price REAL NOT NULL,
                PRIMARY KEY (guild_id, symbol, date)
            );
            """
        )
        _ensure_users_bank_type(conn)
        _ensure_rank_column(conn)
        _ensure_company_columns(conn)
        _ensure_price_history_columns(conn)


def _ensure_rank_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if "rank" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN rank TEXT NOT NULL DEFAULT 'PRIVATE';"
        )


def _ensure_users_bank_type(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]: row["type"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if not columns:
        return
    if columns.get("bank", "").upper() == "REAL":
        return

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users_new (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            bank REAL NOT NULL,
            rank TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );

        INSERT INTO users_new (
            guild_id,
            user_id,
            joined_at,
            bank,
            rank
        )
        SELECT
            guild_id,
            user_id,
            joined_at,
            bank,
            rank
        FROM users;

        DROP TABLE users;
        ALTER TABLE users_new RENAME TO users;
        """
    )


def _ensure_company_columns(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(companies);").fetchall()
    }
    if not columns:
        return
    if "guild_id" not in columns:
        _migrate_companies_table(conn)
        return
    if "offset" in columns:
        _migrate_companies_remove_offset(conn)
        return
    if "starting_tick" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN starting_tick INTEGER NOT NULL DEFAULT 0;"
        )
    if "current_price" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN current_price REAL NOT NULL DEFAULT 0;"
        )
    if "last_tick" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN last_tick INTEGER NOT NULL DEFAULT 0;"
        )
    if "player_impact" not in columns:
        conn.execute(
            "ALTER TABLE companies ADD COLUMN player_impact REAL NOT NULL DEFAULT 0.5;"
        )


def _ensure_price_history_columns(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(price_history);").fetchall()
    }
    if not columns:
        return
    if "tick_index" not in columns:
        _migrate_price_history_table(conn)
        return
    if "guild_id" not in columns:
        conn.execute(
            "ALTER TABLE price_history ADD COLUMN guild_id INTEGER NOT NULL DEFAULT 0;"
        )


def _migrate_companies_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS companies_new (
            guild_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            base_price REAL NOT NULL,
            slope REAL NOT NULL,
            drift REAL NOT NULL,
            offset REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, symbol)
        );

        INSERT INTO companies_new (
            guild_id,
            symbol,
            name,
            base_price,
            slope,
            drift,
            offset,
            updated_at
        )
        SELECT
            0 AS guild_id,
            symbol,
            name,
            base_price,
            slope,
            drift,
            offset,
            updated_at
        FROM companies;

        DROP TABLE companies;
        ALTER TABLE companies_new RENAME TO companies;
        """
    )


def _migrate_companies_remove_offset(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS companies_new (
            guild_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            base_price REAL NOT NULL,
            slope REAL NOT NULL,
            drift REAL NOT NULL,
            starting_tick INTEGER NOT NULL,
            current_price REAL NOT NULL DEFAULT 0,
            last_tick INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, symbol)
        );

        INSERT INTO companies_new (
            guild_id,
            symbol,
            name,
            base_price,
            slope,
            drift,
            starting_tick,
            current_price,
            last_tick,
            updated_at
        )
        SELECT
            guild_id,
            symbol,
            name,
            base_price,
            slope,
            drift,
            starting_tick,
            current_price,
            last_tick,
            updated_at
        FROM companies;

        DROP TABLE companies;
        ALTER TABLE companies_new RENAME TO companies;
        """
    )


def _migrate_price_history_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS price_history_new (
            guild_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            tick_index INTEGER NOT NULL,
            ts TEXT NOT NULL,
            price REAL NOT NULL,
            PRIMARY KEY (guild_id, symbol, tick_index)
        );

        INSERT INTO price_history_new (
            guild_id,
            symbol,
            tick_index,
            ts,
            price
        )
        SELECT
            COALESCE(guild_id, 0) AS guild_id,
            symbol,
            CAST(strftime('%s', ts) AS INTEGER) AS tick_index,
            ts,
            price
        FROM price_history;

        DROP TABLE price_history;
        ALTER TABLE price_history_new RENAME TO price_history;
        """
    )
