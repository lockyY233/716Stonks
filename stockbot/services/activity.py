from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Callable

from stockbot.db.database import get_connection


def active_user_ids_last_day(
    guild_id: int,
    *,
    now_epoch: float | None = None,
    connection_factory: Callable = get_connection,
) -> set[int]:
    now = float(now_epoch) if now_epoch is not None else time.time()
    threshold_epoch = now - 86400.0
    since_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

    with connection_factory() as conn:
        state_rows = conn.execute(
            """
            SELECT key, value
            FROM app_state
            WHERE key LIKE ?
            """,
            (f"user_last_active:{guild_id}:%",),
        ).fetchall()
        action_rows = conn.execute(
            """
            SELECT DISTINCT user_id
            FROM action_history
            WHERE guild_id = ? AND created_at >= ?
            """,
            (guild_id, since_iso),
        ).fetchall()

    active_ids = {int(row["user_id"]) for row in action_rows}
    for row in state_rows:
        key = str(row["key"])
        try:
            user_id = int(key.rsplit(":", 1)[-1])
            last_epoch = float(row["value"])
        except (TypeError, ValueError):
            continue
        if last_epoch >= threshold_epoch:
            active_ids.add(user_id)
    return active_ids
