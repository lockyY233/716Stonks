from __future__ import annotations

import json
from random import Random

from stockbot.config.runtime import get_app_config
from stockbot.core.commodity_rarity import normalize_rarity
from stockbot.db import get_commodities, get_state_value, set_state_value

_SHOP_COUNT = 5


def current_shop_bucket() -> int:
    raw = get_state_value("last_tick")
    tick = int(raw) if raw is not None else 0
    # Shop rotation is tick-bound (refreshes each market tick), not tied to
    # trading-limit reset windows.
    return max(0, tick)


def _items_key(guild_id: int, bucket: int) -> str:
    return f"shop_items:{guild_id}:{bucket}"


def _nonce_key(guild_id: int, bucket: int) -> str:
    return f"shop_nonce:{guild_id}:{bucket}"


def sold_key(guild_id: int, bucket: int, commodity_name: str) -> str:
    return f"shop_sold:{guild_id}:{bucket}:{commodity_name.strip().lower()}"


def _load_rarity_weights() -> dict[str, float]:
    raw = str(get_app_config("SHOP_RARITY_WEIGHTS") or "").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    weights: dict[str, float] = {}
    for key, value in parsed.items():
        rarity = normalize_rarity(str(key))
        try:
            weights[rarity] = max(0.0, float(value))
        except (TypeError, ValueError):
            continue
    if not weights:
        return {
            "common": 1.0,
            "uncommon": 0.6,
            "rare": 0.3,
            "legendary": 0.1,
            "exotic": 0.03,
        }
    return weights


def _item_weight(item: dict, rarity_weights: dict[str, float]) -> float:
    override = float(item.get("spawn_weight_override", 0.0) or 0.0)
    if override > 0:
        return override
    rarity = normalize_rarity(str(item.get("rarity", "common")))
    return max(0.0, float(rarity_weights.get(rarity, 0.0)))


def _weighted_pick_without_replacement(
    rows: list[dict],
    *,
    count: int,
    rng: Random,
    rarity_weights: dict[str, float],
) -> list[dict]:
    pool = [dict(r) for r in rows]
    picked: list[dict] = []
    while pool and len(picked) < count:
        weights = [_item_weight(item, rarity_weights) for item in pool]
        total = sum(weights)
        if total <= 0:
            idx = int(rng.random() * len(pool))
        else:
            needle = rng.random() * total
            acc = 0.0
            idx = 0
            for i, w in enumerate(weights):
                acc += w
                if needle <= acc:
                    idx = i
                    break
        picked.append(pool.pop(idx))
    return picked


def _parse_saved_names(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    names: list[str] = []
    for x in data:
        name = str(x).strip()
        if name and name not in names:
            names.append(name)
    return names


def _build_or_load_shop_names(guild_id: int, bucket: int, rows: list[dict]) -> list[str]:
    by_name = {str(r.get("name", "")).lower(): str(r.get("name", "")) for r in rows}
    saved_names = _parse_saved_names(get_state_value(_items_key(guild_id, bucket)))
    names = [n for n in saved_names if n.lower() in by_name]

    if len(names) >= _SHOP_COUNT:
        return names[:_SHOP_COUNT]

    nonce_raw = get_state_value(_nonce_key(guild_id, bucket))
    try:
        nonce = int(nonce_raw) if nonce_raw is not None else 0
    except ValueError:
        nonce = 0
    rarity_weights = _load_rarity_weights()
    rng = Random(f"shop:{guild_id}:{bucket}:{nonce}")

    taken = {n.lower() for n in names}
    remaining_rows = [r for r in rows if str(r.get("name", "")).lower() not in taken]
    fill = _weighted_pick_without_replacement(
        remaining_rows,
        count=max(0, _SHOP_COUNT - len(names)),
        rng=rng,
        rarity_weights=rarity_weights,
    )
    names.extend(str(r.get("name", "")).strip() for r in fill if str(r.get("name", "")).strip())
    names = names[:_SHOP_COUNT]
    set_state_value(_items_key(guild_id, bucket), json.dumps(names, separators=(",", ":")))
    return names


def get_shop_items(guild_id: int) -> tuple[int, list[dict], list[str]]:
    bucket = current_shop_bucket()
    rows = get_commodities(guild_id)
    if not rows:
        return bucket, [], []

    by_name = {str(r.get("name", "")).lower(): dict(r) for r in rows}
    names = _build_or_load_shop_names(guild_id, bucket, rows)
    items: list[dict] = []
    for name in names:
        row = by_name.get(name.lower())
        if not row:
            continue
        item = dict(row)
        s_key = sold_key(guild_id, bucket, str(item.get("name", "")))
        sold_raw = get_state_value(s_key)
        sold = int(sold_raw) if sold_raw and sold_raw.isdigit() else 0
        item["in_stock"] = sold < 1
        item["_stock_key"] = s_key
        items.append(item)
    available = sorted({str(r.get("name", "")) for r in rows if str(r.get("name", "")).strip()})
    return bucket, items, available


def set_item_availability(guild_id: int, commodity_name: str, in_stock: bool) -> None:
    bucket = current_shop_bucket()
    key = sold_key(guild_id, bucket, commodity_name)
    set_state_value(key, "0" if in_stock else "1")


def swap_item(guild_id: int, slot_index: int, new_commodity_name: str) -> bool:
    bucket = current_shop_bucket()
    rows = get_commodities(guild_id)
    valid_names = {str(r.get("name", "")).strip().lower() for r in rows}
    new_name = str(new_commodity_name).strip()
    if not new_name or new_name.lower() not in valid_names:
        return False

    names = _build_or_load_shop_names(guild_id, bucket, rows)
    idx = int(slot_index)
    if idx < 0 or idx >= len(names):
        return False
    if new_name.lower() in {n.lower() for i, n in enumerate(names) if i != idx}:
        return False
    names[idx] = new_name
    set_state_value(_items_key(guild_id, bucket), json.dumps(names, separators=(",", ":")))
    return True


def refresh_shop(guild_id: int) -> None:
    bucket = current_shop_bucket()
    nonce_raw = get_state_value(_nonce_key(guild_id, bucket))
    try:
        nonce = int(nonce_raw) if nonce_raw is not None else 0
    except ValueError:
        nonce = 0
    set_state_value(_nonce_key(guild_id, bucket), str(nonce + 1))
    set_state_value(_items_key(guild_id, bucket), "[]")
    # Force generation immediately so dashboard sees fresh set now.
    get_shop_items(guild_id)
