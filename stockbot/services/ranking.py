from __future__ import annotations

from stockbot.db import get_state_value, get_users
from stockbot.services.perks import evaluate_user_perks

MIN_RANKING_NETWORTH = 100.0
MIN_RANKING_NOTE = "You must have networth > $100 to be listed on the ranking."


def get_ranked_users_with_effective_networth(
    guild_id: int,
    *,
    limit: int = 50,
    exclude_user_id: int = 0,
    min_networth: float = MIN_RANKING_NETWORTH,
) -> list[dict]:
    rows = get_users(guild_id)
    ranked: list[dict] = []
    for row in rows:
        user_id = int(row.get("user_id", 0))
        if exclude_user_id > 0 and user_id == int(exclude_user_id):
            continue
        base_networth = float(row.get("networth", 0.0))
        eval_result = evaluate_user_perks(
            guild_id=guild_id,
            user_id=user_id,
            base_income=0.0,
            base_trade_limits=0,
            base_networth=base_networth,
        )
        effective_networth = float(eval_result["final"]["networth"])
        bonus_raw = get_state_value(f"daily_networth_bonus:{guild_id}:{user_id}")
        try:
            top_networth_bonus = float(bonus_raw) if bonus_raw is not None else 0.0
        except (TypeError, ValueError):
            top_networth_bonus = 0.0
        next_row = dict(row)
        next_row["display_base_networth"] = effective_networth - top_networth_bonus
        next_row["top_networth_bonus"] = top_networth_bonus
        next_row["networth"] = effective_networth
        ranked.append(next_row)

    ranked = [row for row in ranked if float(row.get("networth", 0.0)) >= float(min_networth)]
    ranked.sort(
        key=lambda r: (
            float(r.get("networth", 0.0)),
            float(r.get("bank", 0.0)),
            -int(r.get("user_id", 0)),
        ),
        reverse=True,
    )
    return ranked[: max(1, int(limit))]
