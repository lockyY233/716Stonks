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
