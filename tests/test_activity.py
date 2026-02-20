import sqlite3
import time
import unittest
from datetime import datetime, timedelta, timezone

from stockbot.services.activity import active_user_ids_last_day


class ActivityTests(unittest.TestCase):
    def _conn_factory(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE action_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        return conn

    def test_includes_recent_state_or_action(self) -> None:
        guild_id = 123
        now = time.time()
        recent_action_iso = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()

        conn = self._conn_factory()
        conn.execute(
            "INSERT INTO app_state (key, value) VALUES (?, ?)",
            (f"user_last_active:{guild_id}:10", str(now - 3600)),
        )
        conn.execute(
            "INSERT INTO app_state (key, value) VALUES (?, ?)",
            (f"user_last_active:{guild_id}:20", str(now - 90000)),
        )
        conn.execute(
            "INSERT INTO action_history (guild_id, user_id, created_at) VALUES (?, ?, ?)",
            (guild_id, 30, recent_action_iso),
        )

        def factory() -> sqlite3.Connection:
            return conn

        users = active_user_ids_last_day(guild_id, now_epoch=now, connection_factory=factory)
        self.assertIn(10, users)
        self.assertIn(30, users)
        self.assertNotIn(20, users)


if __name__ == "__main__":
    unittest.main()
