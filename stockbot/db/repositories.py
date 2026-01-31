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
