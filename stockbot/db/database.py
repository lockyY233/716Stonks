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
                bank INTEGER NOT NULL,
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
            """
        )
        _ensure_rank_column(conn)


def _ensure_rank_column(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users);").fetchall()
    }
    if "rank" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN rank TEXT NOT NULL DEFAULT 'PRIVATE';"
        )
