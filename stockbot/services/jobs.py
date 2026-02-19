from __future__ import annotations

import json
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from stockbot.db import get_jobs
from stockbot.db import (
    add_action_history,
    clear_state_prefix,
    get_job,
    get_state_value,
    get_user,
    get_user_job_state,
    count_user_running_timed_jobs,
    list_running_timed_jobs,
    get_user_running_timed_job,
    set_state_value,
    set_user_job_state,
    update_user_bank,
)
from stockbot.services.perks import evaluate_user_perks


@dataclass(frozen=True)
class JobResult:
    ok: bool
    message: str
    reward: float = 0.0


def current_tick() -> int:
    raw = get_state_value("last_tick")
    try:
        return max(0, int(raw)) if raw is not None else 0
    except (TypeError, ValueError):
        return 0


def _jobs_key(guild_id: int, bucket: int) -> str:
    return f"jobs_items:{guild_id}:{bucket}"


def _nonce_key(guild_id: int, bucket: int) -> str:
    return f"jobs_nonce:{guild_id}:{bucket}"


def _unavailable_key(guild_id: int, bucket: int, job_id: int) -> str:
    return f"jobs_unavailable:{guild_id}:{bucket}:{job_id}"


def _parse_saved_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: list[int] = []
    seen: set[int] = set()
    for item in data:
        try:
            jid = int(item)
        except (TypeError, ValueError):
            continue
        if jid <= 0 or jid in seen:
            continue
        seen.add(jid)
        out.append(jid)
    return out


def _build_or_load_job_ids(guild_id: int, bucket: int, rows: list[dict], limit: int) -> list[int]:
    valid_ids = {int(r.get("id", 0)) for r in rows}
    saved_ids = _parse_saved_ids(get_state_value(_jobs_key(guild_id, bucket)))
    job_ids = [jid for jid in saved_ids if jid in valid_ids]
    limit = max(1, int(limit))
    if len(job_ids) >= limit:
        return job_ids[:limit]

    nonce_raw = get_state_value(_nonce_key(guild_id, bucket))
    try:
        nonce = int(nonce_raw) if nonce_raw is not None else 0
    except ValueError:
        nonce = 0
    rng = random.Random(f"jobs:{guild_id}:{bucket}:{nonce}")

    taken = set(job_ids)
    remaining_ids = [int(r.get("id", 0)) for r in rows if int(r.get("id", 0)) not in taken]
    rng.shuffle(remaining_ids)
    job_ids.extend(remaining_ids[: max(0, limit - len(job_ids))])
    job_ids = job_ids[:limit]
    set_state_value(_jobs_key(guild_id, bucket), json.dumps(job_ids, separators=(",", ":")))
    return job_ids


def get_active_jobs(guild_id: int, limit: int = 5) -> list[dict]:
    rows = get_jobs(guild_id, enabled_only=True)
    bucket = current_tick()

    def _with_availability(items: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in items:
            item = dict(row)
            jid = int(item.get("id", 0))
            taken = get_state_value(_unavailable_key(guild_id, bucket, jid))
            item["available_this_tick"] = str(taken or "0") != "1"
            out.append(item)
        return out

    if len(rows) <= max(1, int(limit)):
        return _with_availability(rows)
    by_id = {int(r.get("id", 0)): dict(r) for r in rows}
    ids = _build_or_load_job_ids(guild_id, bucket, rows, max(1, int(limit)))
    out: list[dict] = []
    for jid in ids:
        row = by_id.get(jid)
        if row:
            out.append(dict(row))
    return _with_availability(out)


def is_job_available_this_tick(guild_id: int, job_id: int) -> bool:
    bucket = current_tick()
    raw = get_state_value(_unavailable_key(guild_id, bucket, int(job_id)))
    return str(raw or "0") != "1"


def mark_job_unavailable_this_tick(guild_id: int, job_id: int) -> None:
    bucket = current_tick()
    set_state_value(_unavailable_key(guild_id, bucket, int(job_id)), "1")


def refresh_jobs_rotation(guild_id: int, limit: int = 5) -> list[dict]:
    bucket = current_tick()
    # Refreshing rotation should also reset the per-tick job availability state.
    clear_state_prefix(f"jobs_unavailable:{guild_id}:{bucket}:")
    nonce_raw = get_state_value(_nonce_key(guild_id, bucket))
    try:
        nonce = int(nonce_raw) if nonce_raw is not None else 0
    except ValueError:
        nonce = 0
    set_state_value(_nonce_key(guild_id, bucket), str(nonce + 1))
    set_state_value(_jobs_key(guild_id, bucket), "[]")
    return get_active_jobs(guild_id, limit=max(1, int(limit)))


def swap_active_job(guild_id: int, slot_index: int, new_job_id: int, limit: int = 5) -> bool:
    rows = get_jobs(guild_id, enabled_only=True)
    valid_ids = {int(r.get("id", 0)) for r in rows}
    if int(new_job_id) not in valid_ids:
        return False
    bucket = current_tick()
    ids = _build_or_load_job_ids(guild_id, bucket, rows, max(1, int(limit)))
    idx = int(slot_index)
    if idx < 0 or idx >= len(ids):
        return False
    if int(new_job_id) in {jid for i, jid in enumerate(ids) if i != idx}:
        return False
    ids[idx] = int(new_job_id)
    set_state_value(_jobs_key(guild_id, bucket), json.dumps(ids, separators=(",", ":")))
    return True


def _roll_range(min_value: float, max_value: float) -> float:
    lo = float(min_value)
    hi = float(max_value)
    if hi < lo:
        lo, hi = hi, lo
    return round(random.uniform(lo, hi), 2)


def _pay_user_for_job(guild_id: int, user_id: int, job: dict, reward: float, details: str) -> float:
    if abs(reward) <= 1e-9:
        return 0.0
    user = get_user(guild_id, user_id)
    if user is None:
        return 0.0
    before_bank = float(user.get("bank", 0.0))
    new_bank = max(0.0, before_bank + float(reward))
    applied = round(new_bank - before_bank, 2)
    if abs(applied) <= 1e-9:
        return 0.0
    update_user_bank(guild_id, user_id, new_bank)
    add_action_history(
        guild_id=guild_id,
        user_id=user_id,
        action_type="job",
        target_type="job",
        target_symbol=str(job.get("name", f"job#{job.get('id', 0)}")),
        quantity=1.0,
        unit_price=float(applied),
        total_amount=float(applied),
        details=details,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    return applied


def _job_config(job: dict) -> dict:
    raw = str(job.get("config_json", "") or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _fmt_template(text: str, **values: object) -> str:
    out = str(text or "")
    for key, value in values.items():
        out = out.replace("{" + key + "}", str(value))
    return out


def _with_payout_line(message: str, amount: float) -> str:
    amt = float(amount)
    if amt > 0:
        line = f"Payout: **+${amt:.2f}**"
    elif amt < 0:
        line = f"Payout: **-${abs(amt):.2f}**"
    else:
        line = "Payout: **$0.00**"
    base = str(message or "").strip()
    if not base:
        return line
    return f"{base}\n{line}"


def _parse_chance_outcomes(job: dict) -> list[dict]:
    cfg = _job_config(job)
    raw = cfg.get("chance_outcomes", [])
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            min_roll = float(item.get("min_roll", 0.0))
            max_roll = float(item.get("max_roll", 100.0))
        except (TypeError, ValueError):
            continue
        if max_roll < min_roll:
            min_roll, max_roll = max_roll, min_roll
        legacy_reward = item.get("reward", None)
        reward_min_raw = item.get("reward_min", legacy_reward if legacy_reward not in (None, "") else 0.0)
        reward_max_raw = item.get("reward_max", legacy_reward if legacy_reward not in (None, "") else reward_min_raw)
        try:
            reward_min = float(reward_min_raw)
        except (TypeError, ValueError):
            reward_min = 0.0
        try:
            reward_max = float(reward_max_raw)
        except (TypeError, ValueError):
            reward_max = reward_min
        out.append(
            {
                "min_roll": min_roll,
                "max_roll": max_roll,
                "reward_min": reward_min,
                "reward_max": reward_max,
                "message": str(item.get("message", "") or ""),
            }
        )
    return out


def _parse_quiz_choices(job: dict) -> list[dict]:
    cfg = _job_config(job)
    raw = cfg.get("quiz_choices", [])
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        label = str(item.get("label", "")).strip()
        value = str(item.get("value", "")).strip()
        if not label:
            continue
        if not value:
            value = f"choice_{idx+1}"
        legacy_reward = item.get("reward", None)
        reward_min_raw = item.get("reward_min", legacy_reward if legacy_reward not in (None, "") else 0.0)
        reward_max_raw = item.get("reward_max", legacy_reward if legacy_reward not in (None, "") else reward_min_raw)
        try:
            reward_min = float(reward_min_raw)
        except (TypeError, ValueError):
            reward_min = 0.0
        try:
            reward_max = float(reward_max_raw)
        except (TypeError, ValueError):
            reward_max = reward_min
        out.append(
            {
                "label": label,
                "value": value,
                "reward_min": reward_min,
                "reward_max": reward_max,
                "message": str(item.get("message", "") or ""),
            }
        )
    return out


def get_quiz_choices(guild_id: int, job_id: int) -> list[dict]:
    job = get_job(guild_id, job_id)
    if job is None or int(job.get("enabled", 0)) != 1:
        return []
    return _parse_quiz_choices(job)


def _timed_lock_message(lock_row: dict) -> str:
    remaining = _timed_remaining_seconds(
        float(lock_row.get("running_until_epoch", 0.0) or 0.0),
        int(lock_row.get("running_until_tick", 0) or 0),
    )
    name = str(lock_row.get("job_name", f"Job #{lock_row.get('job_id', '?')}"))
    return (
        f"You already have a timed job in progress (**{name}**). "
        f"{(remaining / 60.0):.2f} minute(s) remaining."
    )


def _timed_remaining_seconds(running_until_epoch: float, running_until_tick: int = 0) -> int:
    if float(running_until_epoch) > 0.0:
        return max(0, int(math.ceil(float(running_until_epoch) - time.time())))
    # Legacy fallback for rows that still only have tick-based end time.
    ticks_remaining = max(0, int(running_until_tick) - current_tick())
    tick_interval = max(1, int(get_app_config("TICK_INTERVAL")))
    return max(0, int(math.ceil(ticks_remaining * tick_interval)))


def _timed_duration_minutes(job: dict) -> float:
    minutes = float(job.get("duration_minutes", 0.0) or 0.0)
    if minutes > 0.0:
        return minutes
    # Legacy fallback for old rows.
    return float(max(1, int(job.get("duration_ticks", 1))))


def _timed_reward_range(job: dict) -> tuple[float, float]:
    cfg = _job_config(job)
    low = float(cfg.get("timed_reward_min", job.get("reward_min", 0.0)) or 0.0)
    high = float(cfg.get("timed_reward_max", job.get("reward_max", low)) or low)
    if high < low:
        low, high = high, low
    return low, high


def abandon_running_timed_job(guild_id: int, user_id: int) -> JobResult:
    lock_row = get_user_running_timed_job(guild_id, user_id, current_tick())
    if lock_row is None:
        return JobResult(False, "No active timed job to abandon.")
    job_id = int(lock_row.get("job_id", 0))
    name = str(lock_row.get("job_name", f"Job #{job_id}"))
    set_user_job_state(
        guild_id,
        user_id,
        job_id,
        next_available_tick=0,
        running_until_tick=0,
        running_until_epoch=0.0,
    )
    return JobResult(True, f"Abandoned timed job **{name}**.")


def try_run_chance_job(guild_id: int, user_id: int, job_id: int) -> JobResult:
    job = get_job(guild_id, job_id)
    if job is None or int(job.get("enabled", 0)) != 1:
        return JobResult(False, "Job unavailable.")
    if not is_job_available_this_tick(guild_id, job_id):
        return JobResult(False, "This job is unavailable until next tick.")
    lock_row = get_user_running_timed_job(guild_id, user_id, current_tick())
    if lock_row is not None:
        return JobResult(False, _timed_lock_message(lock_row))
    roll_pct = random.uniform(0.0, 100.0)
    chance_eval = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=0,
        base_networth=0.0,
        base_commodities_limit=0,
        base_job_slots=1,
        base_timed_duration=0.0,
        base_chance_roll_bonus=0.0,
    )
    roll_bonus = float(chance_eval["final"]["chance_roll_bonus"])
    effective_roll = max(0.0, min(99.9999, roll_pct + roll_bonus))
    custom = None
    for outcome in _parse_chance_outcomes(job):
        if float(outcome["min_roll"]) <= effective_roll < float(outcome["max_roll"]):
            custom = outcome
            break
    set_user_job_state(
        guild_id,
        user_id,
        job_id,
        next_available_tick=0,
        running_until_tick=0,
        running_until_epoch=0.0,
    )
    if custom is not None:
        reward = _roll_range(float(custom.get("reward_min", 0.0)), float(custom.get("reward_max", 0.0)))
        applied = _pay_user_for_job(
            guild_id,
            user_id,
            job,
            reward,
            f"type=chance;roll={roll_pct:.2f};roll_bonus={roll_bonus:.2f};effective_roll={effective_roll:.2f};custom=1",
        )
        mark_job_unavailable_this_tick(guild_id, job_id)
        msg = str(custom["message"] or "").strip()
        if not msg:
            if applied >= 0:
                msg = f"Outcome triggered ({effective_roll:.2f}). You earned **${applied:.2f}**."
            else:
                msg = f"Outcome triggered ({effective_roll:.2f}). You lost **${abs(applied):.2f}**."
        msg = _fmt_template(msg, roll=f"{effective_roll:.2f}", reward=f"{applied:.2f}")
        return JobResult(applied >= 0, _with_payout_line(msg, applied), reward=applied)

    # Chance jobs are fully outcome-driven; without a matching custom outcome,
    # the attempt resolves with no payout.
    applied = _pay_user_for_job(guild_id, user_id, job, 0.0, "type=chance;outcome=none")
    mark_job_unavailable_this_tick(guild_id, job_id)
    return JobResult(
        False,
        _with_payout_line("No outcome matched this roll.", applied),
        reward=applied,
    )


def try_start_or_claim_timed_job(guild_id: int, user_id: int, job_id: int) -> JobResult:
    job = get_job(guild_id, job_id)
    if job is None or int(job.get("enabled", 0)) != 1:
        return JobResult(False, "Job unavailable.")
    lock_row = get_user_running_timed_job(guild_id, user_id, current_tick())
    if lock_row is not None and int(lock_row.get("job_id", 0)) != int(job_id):
        return JobResult(False, _timed_lock_message(lock_row))
    state = get_user_job_state(guild_id, user_id, job_id)
    running_until_epoch = float(state.get("running_until_epoch", 0.0) or 0.0)
    running_until_tick = int(state.get("running_until_tick", 0) or 0)
    has_running = running_until_epoch > 0.0 or running_until_tick > 0
    remaining_seconds = _timed_remaining_seconds(running_until_epoch, running_until_tick) if has_running else 0
    if has_running and remaining_seconds > 0:
        return JobResult(False, f"Job in progress. {(remaining_seconds / 60.0):.2f} minute(s) remaining.")
    if has_running and remaining_seconds <= 0:
        cfg = _job_config(job)
        reward = _roll_range(
            float(cfg.get("timed_reward_min", 0.0)),
            float(cfg.get("timed_reward_max", 0.0)),
        )
        set_user_job_state(
            guild_id,
            user_id,
            job_id,
            next_available_tick=0,
            running_until_tick=0,
            running_until_epoch=0.0,
        )
        applied = _pay_user_for_job(guild_id, user_id, job, reward, "type=timed;result=claim")
        claim_msg = str(cfg.get("timed_claim_message", "") or "").strip()
        if not claim_msg:
            claim_msg = f"Timed job completed. You earned **${applied:.2f}**."
        duration_minutes = _timed_duration_minutes(job)
        claim_msg = _fmt_template(claim_msg, reward=f"{applied:.2f}", duration=f"{duration_minutes:.2f}")
        return JobResult(True, _with_payout_line(claim_msg, applied), reward=applied)
    if not is_job_available_this_tick(guild_id, job_id):
        return JobResult(False, "This job is unavailable until next tick.")
    slots_eval = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=0,
        base_networth=0.0,
        base_commodities_limit=0,
        base_job_slots=1,
    )
    max_slots = max(0, int(slots_eval["final"]["job_slots"]))
    now_tick = current_tick()
    running_count = count_user_running_timed_jobs(guild_id, user_id, now_tick)
    if max_slots <= 0 or running_count >= max_slots:
        return JobResult(
            False,
            f"Job slot limit reached ({running_count}/{max_slots}).",
        )
    base_duration_minutes = _timed_duration_minutes(job)
    duration_eval = evaluate_user_perks(
        guild_id=guild_id,
        user_id=user_id,
        base_income=0.0,
        base_trade_limits=0,
        base_networth=0.0,
        base_commodities_limit=0,
        base_job_slots=1,
        base_timed_duration=base_duration_minutes,
        base_chance_roll_bonus=0.0,
    )
    duration_minutes = max(0.1, float(duration_eval["final"]["timed_duration"]))
    end_epoch = time.time() + (duration_minutes * 60.0)
    set_user_job_state(
        guild_id,
        user_id,
        job_id,
        next_available_tick=0,
        running_until_tick=0,
        running_until_epoch=end_epoch,
    )
    mark_job_unavailable_this_tick(guild_id, job_id)
    cfg = _job_config(job)
    start_msg = str(cfg.get("timed_start_message", "") or "").strip()
    if not start_msg:
        start_msg = "Timed job started."
    start_msg = _fmt_template(start_msg, duration=f"{duration_minutes:.2f}")
    est_min, est_max = _timed_reward_range(job)
    remaining_minutes = max(0.0, (end_epoch - time.time()) / 60.0)
    return JobResult(
        True,
        (
            f"{start_msg}\n"
            f"Estimated payout: **${est_min:.2f} - ${est_max:.2f}**\n"
            f"Time remaining: **{remaining_minutes:.2f} minute(s)**"
        ),
    )


def submit_quiz_choice(guild_id: int, user_id: int, job_id: int, choice_value: str) -> JobResult:
    job = get_job(guild_id, job_id)
    if job is None or int(job.get("enabled", 0)) != 1:
        return JobResult(False, "Job unavailable.")
    if not is_job_available_this_tick(guild_id, job_id):
        return JobResult(False, "This job is unavailable until next tick.")
    lock_row = get_user_running_timed_job(guild_id, user_id, current_tick())
    if lock_row is not None:
        return JobResult(False, _timed_lock_message(lock_row))
    choices = _parse_quiz_choices(job)
    if not choices:
        return JobResult(False, "Quiz choices are not configured.")
    provided = str(choice_value or "").strip()
    selected = next((c for c in choices if str(c.get("value", "")) == provided), None)
    if selected is None:
        return JobResult(False, "Invalid choice.")
    set_user_job_state(
        guild_id,
        user_id,
        job_id,
        next_available_tick=0,
        running_until_tick=0,
        running_until_epoch=0.0,
    )
    reward = _roll_range(float(selected.get("reward_min", 0.0)), float(selected.get("reward_max", 0.0)))
    applied = _pay_user_for_job(guild_id, user_id, job, reward, "type=quiz;result=choice")
    mark_job_unavailable_this_tick(guild_id, job_id)
    msg = str(selected.get("message", "") or "").strip()
    if not msg:
        if applied >= 0:
            msg = f"Choice submitted. You earned **${applied:.2f}**."
        else:
            msg = f"Choice submitted. You lost **${abs(applied):.2f}**."
    msg = _fmt_template(msg, reward=f"{applied:.2f}")
    return JobResult(applied >= 0, _with_payout_line(msg, applied), reward=applied)


def process_due_timed_jobs(guild_id: int | None = None, limit: int = 200) -> list[dict]:
    events: list[dict] = []
    rows = list_running_timed_jobs(guild_id=guild_id, limit=limit)
    for row in rows:
        g_id = int(row.get("guild_id", 0))
        u_id = int(row.get("user_id", 0))
        j_id = int(row.get("job_id", 0))
        running_until_epoch = float(row.get("running_until_epoch", 0.0) or 0.0)
        running_until_tick = int(row.get("running_until_tick", 0) or 0)
        if g_id <= 0 or u_id <= 0 or j_id <= 0:
            continue
        remaining = _timed_remaining_seconds(running_until_epoch, running_until_tick)
        if remaining > 0:
            continue
        result = try_start_or_claim_timed_job(g_id, u_id, j_id)
        if not result.ok:
            continue
        events.append(
            {
                "guild_id": g_id,
                "user_id": u_id,
                "job_id": j_id,
                "job_name": str(row.get("job_name", f"Job #{j_id}")),
                "message": str(result.message),
                "reward": float(result.reward),
            }
        )
    return events
