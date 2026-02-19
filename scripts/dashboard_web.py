from __future__ import annotations

import os
import secrets
import sqlite3
import sys
import time
import json
import io
import hashlib
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from flask import Flask, g, jsonify, render_template_string, request, send_file
from PIL import Image, UnidentifiedImageError

# Ensure project root is importable when running as a standalone script via systemd.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEMPLATE_DIR = Path(__file__).resolve().parent / "dashboard_templates"

def _load_template(name: str) -> str:
    return (TEMPLATE_DIR / name).read_text(encoding="utf-8")

from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME
from stockbot.services.jobs import get_active_jobs, refresh_jobs_rotation, swap_active_job
from stockbot.services.perks import evaluate_user_perks
from stockbot.services.shop_state import get_shop_items, refresh_shop, set_item_availability, swap_item

@dataclass(frozen=True)
class _CfgSpec:
    default: object
    cast: type
    description: str


APP_CONFIG_SPECS: dict[str, _CfgSpec] = {
    "START_BALANCE": _CfgSpec(5.0, float, "Starting cash for new users."),
    "TICK_INTERVAL": _CfgSpec(5, int, "Seconds between market ticks."),
    "TREND_MULTIPLIER": _CfgSpec(1e-2, float, "Multiplier used by trend-related math."),
    "DISPLAY_TIMEZONE": _CfgSpec("America/New_York", str, "Timezone used for display and market-close checks."),
    "MARKET_CLOSE_HOUR": _CfgSpec(21.0, float, "Local close time as decimal hour [0,24). Example: 21.5 means 21:30."),
    "STONKERS_ROLE_NAME": _CfgSpec("📈💰📊Stonkers", str, "Role granted on registration and pinged on close updates."),
    "ANNOUNCEMENT_CHANNEL_ID": _CfgSpec(0, int, "Discord channel ID used for announcements; 0 means auto-pick."),
    "ANNOUNCE_MENTION_ROLE": _CfgSpec(1, int, "1 to mention role at close; 0 to disable mention."),
    "GM_ID": _CfgSpec(0, int, "Discord user ID designated as game master; excluded from rankings when >0."),
    "DRIFT_NOISE_FREQUENCY": _CfgSpec(0.7, float, "Normalized fast noise frequency [0,1]."),
    "DRIFT_NOISE_GAIN": _CfgSpec(0.8, float, "Fast noise gain multiplier."),
    "DRIFT_NOISE_LOW_FREQ_RATIO": _CfgSpec(0.08, float, "Low-band frequency ratio relative to fast frequency."),
    "DRIFT_NOISE_LOW_GAIN": _CfgSpec(3.0, float, "Low-band noise gain multiplier."),
    "TRADING_LIMITS": _CfgSpec(40, int, "Max shares traded per period; <=0 disables limits."),
    "TRADING_LIMITS_PERIOD": _CfgSpec(60, int, "Tick window length for trading-limit reset."),
    "TRADING_FEES": _CfgSpec(1.0, float, "Sell fee percentage applied to realized profit."),
    "OWNER_BUY_FEE_RATE": _CfgSpec(5.0, float, "Owner payout percentage from non-owner buy transaction value."),
    "COMMODITIES_LIMIT": _CfgSpec(5, int, "Max total commodity units a player can hold; <=0 disables."),
    "PAWN_SELL_RATE": _CfgSpec(75.0, float, "Pawn payout percentage when selling commodities to bank."),
    "SHOP_RARITY_WEIGHTS": _CfgSpec('{"common":1.0,"uncommon":0.6,"rare":0.3,"legendary":0.1,"exotic":0.03}', str, "JSON mapping of rarity->weight used for shop rotation."),
    "IMAGE_UPLOAD_MAX_SOURCE_MB": _CfgSpec(8, int, "Max download size (MB) allowed when ingesting image URLs."),
    "IMAGE_UPLOAD_MAX_DIM": _CfgSpec(1024, int, "Max width/height in pixels for ingested images."),
    "IMAGE_UPLOAD_MAX_STORED_KB": _CfgSpec(300, int, "Max stored file size in KB per ingested image."),
    "IMAGE_UPLOAD_TOTAL_GB": _CfgSpec(2.0, float, "Total local uploads quota in GB for dashboard-hosted images."),
}

AUTH_REQUIRED_HTML = _load_template("auth_required.html")


MAIN_HTML = _load_template("main.html")


DETAIL_HTML = _load_template("company_detail.html")

COMMODITY_DETAIL_HTML = _load_template("commodity_detail.html")

PLAYER_DETAIL_HTML = _load_template("player_detail.html")

APP_CONFIG_DETAIL_HTML = _load_template("app_config_detail.html")

PERK_DETAIL_HTML = _load_template("perk_detail.html")

JOB_DETAIL_HTML = _load_template("job_detail.html")


def create_app() -> Flask:
    app = Flask(__name__)
    repo_root = Path(__file__).resolve().parents[1]
    db_path = Path(os.getenv("DASHBOARD_DB_PATH", str(repo_root / "data" / "stockbot.db")))
    auth_cookie = "webadmin_session"
    session_ttl_seconds = max(300, int(os.getenv("WEBADMIN_SESSION_TTL_SECONDS", "43200")))
    godtoken_env = (os.getenv("WEBADMIN_GODTOKEN") or os.getenv("GODTOKEN") or "GODTOKEN").strip()

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _state_get(key: str) -> str | None:
        with _connect() as conn:
            row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def _state_set(key: str, value: str) -> None:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def _state_delete(key: str) -> None:
        with _connect() as conn:
            conn.execute("DELETE FROM app_state WHERE key = ?", (key,))

    def _active_user_ids_last_day(guild_id: int) -> set[int]:
        threshold_epoch = time.time() - 86400.0
        since_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        with _connect() as conn:
            state_rows = conn.execute(
                """
                SELECT key, value
                FROM app_state
                WHERE key LIKE ?
                """,
                (f"user_last_active:{guild_id}:%",),
            ).fetchall()
            history_rows = conn.execute(
                """
                SELECT DISTINCT user_id
                FROM action_history
                WHERE guild_id = ? AND created_at >= ?
                """,
                (guild_id, since_iso),
            ).fetchall()
        active_ids = {int(row["user_id"]) for row in history_rows}
        for row in state_rows:
            key = str(row["key"])
            try:
                uid = int(key.rsplit(":", 1)[-1])
                last_epoch = float(row["value"])
            except (TypeError, ValueError):
                continue
            if last_epoch >= threshold_epoch:
                active_ids.add(uid)
        return active_ids

    def _cleanup_expired_auth_entries() -> None:
        now_epoch = int(time.time())
        with _connect() as conn:
            conn.execute(
                """
                DELETE FROM app_state
                WHERE (key LIKE 'webadmin:token:%' OR key LIKE 'webadmin:session:%')
                  AND CAST(value AS INTEGER) < ?
                """,
                (now_epoch,),
            )

    def _consume_one_time_token(token: str) -> bool:
        key = f"webadmin:token:{token}"
        now_epoch = int(time.time())
        with _connect() as conn:
            row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
            if row is None:
                return False
            try:
                expires_at = int(str(row["value"]))
            except ValueError:
                conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
                return False
            if expires_at < now_epoch:
                conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
                return False
            conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
            conn.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (f"webadmin:grace:{token}", str(now_epoch + 120)),
            )
            return True

    def _token_in_grace(token: str) -> bool:
        raw = _state_get(f"webadmin:grace:{token}")
        if raw is None:
            return False
        try:
            return int(raw) >= int(time.time())
        except ValueError:
            return False

    def _create_session() -> str:
        sid = secrets.token_urlsafe(32)
        expires_at = int(time.time()) + session_ttl_seconds
        _state_set(f"webadmin:session:{sid}", str(expires_at))
        return sid

    def _session_valid(session_id: str | None) -> bool:
        if not session_id:
            return False
        raw = _state_get(f"webadmin:session:{session_id}")
        if raw is None:
            return False
        try:
            return int(raw) >= int(time.time())
        except ValueError:
            return False

    def _config_key(name: str) -> str:
        return f"config:{name}"

    def _normalize_config(name: str, value):
        if name in {
            "START_BALANCE",
            "MARKET_CLOSE_HOUR",
            "DRIFT_NOISE_FREQUENCY",
            "DRIFT_NOISE_GAIN",
            "DRIFT_NOISE_LOW_FREQ_RATIO",
            "DRIFT_NOISE_LOW_GAIN",
            "TRADING_FEES",
            "TREND_MULTIPLIER",
            "PAWN_SELL_RATE",
            "OWNER_BUY_FEE_RATE",
            "IMAGE_UPLOAD_TOTAL_GB",
        }:
            number = float(value)
            if name == "MARKET_CLOSE_HOUR":
                return max(0.0, min(23.9997222222, number))
            if name == "DRIFT_NOISE_FREQUENCY":
                return max(0.0, min(1.0, number))
            if name == "PAWN_SELL_RATE":
                return max(0.0, min(100.0, number))
            if name == "OWNER_BUY_FEE_RATE":
                return max(0.0, min(100.0, number))
            if name == "IMAGE_UPLOAD_TOTAL_GB":
                return max(0.1, number)
            if name in {"START_BALANCE", "DRIFT_NOISE_GAIN", "DRIFT_NOISE_LOW_FREQ_RATIO", "DRIFT_NOISE_LOW_GAIN", "TRADING_FEES"}:
                return max(0.0, number)
            return number
        if name == "SHOP_RARITY_WEIGHTS":
            text = str(value).strip()
            if not text:
                return str(APP_CONFIG_SPECS[name].default)
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                raise ValueError("SHOP_RARITY_WEIGHTS must be valid JSON object.")
            if not isinstance(parsed, dict):
                raise ValueError("SHOP_RARITY_WEIGHTS must be a JSON object.")
            normalized: dict[str, float] = {}
            for key, raw in parsed.items():
                k = str(key).strip().lower()
                if not k:
                    continue
                try:
                    normalized[k] = max(0.0, float(raw))
                except (TypeError, ValueError):
                    continue
            if not normalized:
                raise ValueError("SHOP_RARITY_WEIGHTS has no valid entries.")
            return json.dumps(normalized, separators=(",", ":"))
        if name in {
            "TICK_INTERVAL",
            "TRADING_LIMITS",
            "TRADING_LIMITS_PERIOD",
            "ANNOUNCEMENT_CHANNEL_ID",
            "ANNOUNCE_MENTION_ROLE",
            "GM_ID",
            "COMMODITIES_LIMIT",
            "IMAGE_UPLOAD_MAX_SOURCE_MB",
            "IMAGE_UPLOAD_MAX_DIM",
            "IMAGE_UPLOAD_MAX_STORED_KB",
        }:
            if name in {"ANNOUNCEMENT_CHANNEL_ID", "GM_ID"}:
                text = str(value).strip()
                if text == "":
                    return 0
                if text.startswith("+"):
                    text = text[1:]
                if not text.isdigit():
                    raise ValueError(f"{name} must be a positive integer Discord ID.")
                return max(0, int(text))

            number = int(float(value))
            if name == "TICK_INTERVAL":
                return max(1, number)
            if name == "TRADING_LIMITS_PERIOD":
                return max(1, number)
            if name == "ANNOUNCE_MENTION_ROLE":
                return 1 if number > 0 else 0
            if name == "COMMODITIES_LIMIT":
                return max(0, number)
            if name == "IMAGE_UPLOAD_MAX_SOURCE_MB":
                return max(1, number)
            if name == "IMAGE_UPLOAD_MAX_DIM":
                return max(128, number)
            if name == "IMAGE_UPLOAD_MAX_STORED_KB":
                return max(64, number)
            return number
        text = str(value).strip()
        return text or str(APP_CONFIG_SPECS[name].default)

    def _ensure_config_defaults() -> None:
        with _connect() as conn:
            for name, spec in APP_CONFIG_SPECS.items():
                row = conn.execute(
                    "SELECT value FROM app_state WHERE key = ?",
                    (_config_key(name),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO app_state (key, value)
                        VALUES (?, ?)
                        """,
                        (_config_key(name), str(_normalize_config(name, spec.default))),
                    )

    def _uploads_dir() -> Path:
        path = repo_root / "data" / "uploads"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _public_base_url() -> str:
        explicit = str(os.getenv("DASHBOARD_PUBLIC_BASE_URL", "")).strip().rstrip("/")
        if explicit:
            return explicit
        return request.host_url.rstrip("/")

    def _media_url_for(filename: str) -> str:
        return f"{_public_base_url()}/media/uploads/{filename}"

    def _filename_from_stored_image_url(image_url: str) -> str | None:
        text = str(image_url or "").strip()
        if not text:
            return None
        parsed = urlparse(text)
        path = parsed.path if parsed.scheme else text
        marker = "/media/uploads/"
        if marker not in path:
            return None
        name = path.split(marker, 1)[1].strip().split("/", 1)[0]
        if not name or "/" in name or "\\" in name or name.startswith("."):
            return None
        return name

    def _is_http_url(text: str) -> bool:
        try:
            parsed = urlparse(str(text).strip())
            return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        except Exception:
            return False

    def _total_uploaded_bytes() -> int:
        total = 0
        for p in _uploads_dir().glob("*"):
            if p.is_file():
                total += p.stat().st_size
        return total

    def _collect_referenced_uploads(conn: sqlite3.Connection) -> set[str]:
        refs: set[str] = set()
        rows = conn.execute(
            """
            SELECT image_url FROM commodities WHERE image_url != ''
            UNION ALL
            SELECT image_url FROM close_news WHERE image_url != ''
            """
        ).fetchall()
        for row in rows:
            name = _filename_from_stored_image_url(str(row["image_url"] or ""))
            if name:
                refs.add(name)
        return refs

    def _gc_uploaded_images(conn: sqlite3.Connection) -> None:
        refs = _collect_referenced_uploads(conn)
        base = _uploads_dir()
        for p in base.glob("*"):
            if not p.is_file():
                continue
            if p.name not in refs:
                try:
                    p.unlink()
                except OSError:
                    pass

    def _download_and_ingest_image(image_url: str) -> str:
        source_limit = max(1, int(_get_config("IMAGE_UPLOAD_MAX_SOURCE_MB"))) * 1024 * 1024
        max_dim = max(128, int(_get_config("IMAGE_UPLOAD_MAX_DIM")))
        stored_limit = max(64, int(_get_config("IMAGE_UPLOAD_MAX_STORED_KB"))) * 1024
        total_limit = int(max(0.1, float(_get_config("IMAGE_UPLOAD_TOTAL_GB"))) * 1024 * 1024 * 1024)

        req = Request(
            image_url,
            headers={
                "User-Agent": "716Stonks-Dashboard/1.0",
                "Accept": "image/*,*/*;q=0.8",
            },
        )
        raw = bytearray()
        try:
            with urlopen(req, timeout=15) as resp:
                content_type = str(resp.headers.get("Content-Type", "")).lower()
                if content_type and "image" not in content_type:
                    raise ValueError("URL did not return an image content-type.")
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    raw.extend(chunk)
                    if len(raw) > source_limit:
                        raise ValueError(f"Image is too large (max {source_limit // (1024 * 1024)}MB source).")
        except URLError as e:
            raise ValueError(f"Failed to download image URL: {e}") from e

        try:
            with Image.open(io.BytesIO(raw)) as img_src:
                if img_src.mode in {"RGBA", "LA", "P"}:
                    img_base = img_src.convert("RGBA")
                else:
                    img_base = img_src.convert("RGB")
        except (UnidentifiedImageError, OSError) as e:
            raise ValueError("Downloaded data is not a valid image.") from e

        if max(img_base.size) > max_dim:
            img_base.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)

        best: bytes | None = None
        working = img_base
        for _attempt in range(7):
            for quality in (86, 80, 74, 68, 62, 56, 50, 44, 38, 32):
                buf = io.BytesIO()
                working.save(buf, format="WEBP", quality=quality, method=6)
                data = buf.getvalue()
                if best is None or len(data) < len(best):
                    best = data
                if len(data) <= stored_limit:
                    best = data
                    break
            if best is not None and len(best) <= stored_limit:
                break
            w, h = working.size
            if w <= 192 or h <= 192:
                break
            next_size = (max(128, int(w * 0.85)), max(128, int(h * 0.85)))
            working = working.resize(next_size, Image.Resampling.LANCZOS)

        if best is None:
            raise ValueError("Unable to convert image.")
        if len(best) > stored_limit:
            raise ValueError(f"Image is still too large after compression (must be <= {stored_limit // 1024}KB).")

        digest = hashlib.sha256(best).hexdigest()[:24]
        filename = f"{digest}.webp"
        dest = _uploads_dir() / filename
        if not dest.exists():
            current_total = _total_uploaded_bytes()
            if current_total + len(best) > total_limit:
                raise ValueError("Upload quota reached. Delete unused images or increase IMAGE_UPLOAD_TOTAL_GB.")
            dest.write_bytes(best)
        return _media_url_for(filename)

    def _normalize_image_url_for_storage(conn: sqlite3.Connection, raw_value: object) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return ""
        filename = _filename_from_stored_image_url(value)
        if filename:
            return _media_url_for(filename)
        if _is_http_url(value):
            stored = _download_and_ingest_image(value)
            return stored
        raise ValueError("image_url must be empty or a valid http/https URL.")

    def _normalize_image_url_with_status(conn: sqlite3.Connection, raw_value: object) -> tuple[str, str]:
        value = str(raw_value or "").strip()
        if not value:
            return "", "cleared"
        filename = _filename_from_stored_image_url(value)
        if filename:
            return _media_url_for(filename), "unchanged"
        if _is_http_url(value):
            stored = _download_and_ingest_image(value)
            return stored, "ingested"
        raise ValueError("image_url must be empty or a valid http/https URL.")

    def _ensure_perk_tables() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS perks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    priority INTEGER NOT NULL DEFAULT 100,
                    stack_mode TEXT NOT NULL DEFAULT 'add',
                    max_stacks INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS perk_requirements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    perk_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL DEFAULT 1,
                    req_type TEXT NOT NULL DEFAULT 'commodity_qty',
                    commodity_name TEXT NOT NULL DEFAULT '',
                    operator TEXT NOT NULL DEFAULT '>=',
                    value INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY (perk_id)
                        REFERENCES perks (id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS perk_effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    perk_id INTEGER NOT NULL,
                    effect_type TEXT NOT NULL DEFAULT 'stat_mod',
                    target_stat TEXT NOT NULL DEFAULT 'income',
                    value_mode TEXT NOT NULL DEFAULT 'flat',
                    value REAL NOT NULL DEFAULT 0,
                    scale_source TEXT NOT NULL DEFAULT 'none',
                    scale_key TEXT NOT NULL DEFAULT '',
                    scale_factor REAL NOT NULL DEFAULT 0,
                    cap REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (perk_id)
                        REFERENCES perks (id)
                        ON DELETE CASCADE
                );
                """
            )
            perk_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(perks);").fetchall()
            }
            if "income_multiplier" in perk_cols:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS perks_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        guild_id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL DEFAULT '',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        priority INTEGER NOT NULL DEFAULT 100,
                        stack_mode TEXT NOT NULL DEFAULT 'add',
                        max_stacks INTEGER NOT NULL DEFAULT 1
                    );
                    INSERT INTO perks_new (id, guild_id, name, description, enabled, priority, stack_mode, max_stacks)
                    SELECT id, guild_id, name, description, enabled, 100, 'add', 1
                    FROM perks;
                    DROP TABLE perks;
                    ALTER TABLE perks_new RENAME TO perks;
                    """
                )
            else:
                if "priority" not in perk_cols:
                    conn.execute("ALTER TABLE perks ADD COLUMN priority INTEGER NOT NULL DEFAULT 100;")
                if "stack_mode" not in perk_cols:
                    conn.execute("ALTER TABLE perks ADD COLUMN stack_mode TEXT NOT NULL DEFAULT 'add';")
                if "max_stacks" not in perk_cols:
                    conn.execute("ALTER TABLE perks ADD COLUMN max_stacks INTEGER NOT NULL DEFAULT 1;")

            req_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(perk_requirements);").fetchall()
            }
            if req_cols and "required_qty" in req_cols:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS perk_requirements_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        perk_id INTEGER NOT NULL,
                        group_id INTEGER NOT NULL DEFAULT 1,
                        req_type TEXT NOT NULL DEFAULT 'commodity_qty',
                        commodity_name TEXT NOT NULL DEFAULT '',
                        operator TEXT NOT NULL DEFAULT '>=',
                        value INTEGER NOT NULL DEFAULT 1,
                        FOREIGN KEY (perk_id)
                            REFERENCES perks (id)
                            ON DELETE CASCADE
                    );
                    INSERT INTO perk_requirements_new (id, perk_id, group_id, req_type, commodity_name, operator, value)
                    SELECT id, perk_id, 1, 'commodity_qty', commodity_name, '>=', required_qty
                    FROM perk_requirements;
                    DROP TABLE perk_requirements;
                    ALTER TABLE perk_requirements_new RENAME TO perk_requirements;
                    """
                )
            # Canonicalize legacy alias to avoid req_type confusion.
            conn.execute(
                """
                UPDATE perk_requirements
                SET req_type = 'any_single_commodity_qty'
                WHERE LOWER(TRIM(req_type)) = 'any_single_commodities_qty'
                """
            )

    def _ensure_commodity_tags_table() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS commodity_tags (
                    guild_id INTEGER NOT NULL,
                    commodity_name TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    PRIMARY KEY (guild_id, commodity_name, tag),
                    FOREIGN KEY (guild_id, commodity_name)
                        REFERENCES commodities (guild_id, name)
                        ON DELETE CASCADE
                );
                """
            )

    def _ensure_jobs_tables() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    job_type TEXT NOT NULL DEFAULT 'chance',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    cooldown_ticks INTEGER NOT NULL DEFAULT 0,
                    reward_min REAL NOT NULL DEFAULT 0,
                    reward_max REAL NOT NULL DEFAULT 0,
                    success_rate REAL NOT NULL DEFAULT 1.0,
                    duration_ticks INTEGER NOT NULL DEFAULT 0,
                    duration_minutes REAL NOT NULL DEFAULT 0,
                    quiz_question TEXT NOT NULL DEFAULT '',
                    quiz_answer TEXT NOT NULL DEFAULT '',
                    config_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    UNIQUE(guild_id, name)
                );

                CREATE TABLE IF NOT EXISTS user_jobs (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    job_id INTEGER NOT NULL,
                    next_available_tick INTEGER NOT NULL DEFAULT 0,
                    running_until_tick INTEGER NOT NULL DEFAULT 0,
                    running_until_epoch REAL NOT NULL DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id, job_id),
                    FOREIGN KEY (job_id)
                        REFERENCES jobs (id)
                    ON DELETE CASCADE
                );
                """
            )
            job_cols = {
                row["name"] for row in conn.execute("PRAGMA table_info(jobs);").fetchall()
            }
            if job_cols and "duration_minutes" not in job_cols:
                conn.execute("ALTER TABLE jobs ADD COLUMN duration_minutes REAL NOT NULL DEFAULT 0;")
                conn.execute(
                    "UPDATE jobs SET duration_minutes = CAST(duration_ticks AS REAL) WHERE duration_minutes <= 0;"
                )
            user_job_cols = {
                row["name"] for row in conn.execute("PRAGMA table_info(user_jobs);").fetchall()
            }
            if user_job_cols and "running_until_epoch" not in user_job_cols:
                conn.execute(
                    "ALTER TABLE user_jobs ADD COLUMN running_until_epoch REAL NOT NULL DEFAULT 0;"
                )

    def _ensure_commodities_columns() -> None:
        with _connect() as conn:
            cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(commodities);").fetchall()
            }
            if cols and "spawn_weight_override" not in cols:
                conn.execute(
                    "ALTER TABLE commodities ADD COLUMN spawn_weight_override REAL NOT NULL DEFAULT 0;"
                )
            if cols and "perk_name" not in cols:
                conn.execute(
                    "ALTER TABLE commodities ADD COLUMN perk_name TEXT NOT NULL DEFAULT '';"
                )
            if cols and "perk_description" not in cols:
                conn.execute(
                    "ALTER TABLE commodities ADD COLUMN perk_description TEXT NOT NULL DEFAULT '';"
                )
            if cols and "perk_min_qty" not in cols:
                conn.execute(
                    "ALTER TABLE commodities ADD COLUMN perk_min_qty INTEGER NOT NULL DEFAULT 1;"
                )
            if cols and "perk_effects_json" not in cols:
                conn.execute(
                    "ALTER TABLE commodities ADD COLUMN perk_effects_json TEXT NOT NULL DEFAULT '';"
                )

    def _ensure_company_owners_table() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS company_owners (
                    guild_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    PRIMARY KEY (guild_id, symbol, user_id),
                    FOREIGN KEY (guild_id, symbol)
                        REFERENCES companies (guild_id, symbol)
                        ON DELETE CASCADE
                );
                """
            )
            conn.executescript(
                """
                INSERT OR IGNORE INTO company_owners (guild_id, symbol, user_id)
                SELECT guild_id, symbol, owner_user_id
                FROM companies
                WHERE owner_user_id > 0;
                """
            )

    def _parse_tags(raw: object) -> list[str]:
        text = str(raw or "")
        parts = [p.strip().lower() for p in text.split(",")]
        tags: list[str] = []
        for tag in parts:
            if not tag:
                continue
            if tag not in tags:
                tags.append(tag)
        return tags

    def _parse_owner_user_ids(raw: object) -> list[int]:
        text = str(raw or "").strip()
        if not text:
            return []
        text = text.replace(";", ",").replace("|", ",")
        tokens = [t.strip() for t in text.split(",")]
        out: list[int] = []
        seen: set[int] = set()
        for token in tokens:
            if not token:
                continue
            if token.startswith("+"):
                token = token[1:]
            if not token.isdigit():
                continue
            uid = int(token)
            if uid <= 0 or uid in seen:
                continue
            seen.add(uid)
            out.append(uid)
        return out

    def _set_company_owners(
        conn: sqlite3.Connection,
        guild_id: int,
        symbol: str,
        owner_user_ids: list[int],
    ) -> None:
        conn.execute(
            """
            DELETE FROM company_owners
            WHERE guild_id = ? AND symbol = ?
            """,
            (guild_id, symbol),
        )
        for uid in owner_user_ids:
            conn.execute(
                """
                INSERT OR IGNORE INTO company_owners (guild_id, symbol, user_id)
                VALUES (?, ?, ?)
                """,
                (guild_id, symbol, uid),
            )

    def _sanitize_stack_mode(raw: object) -> str:
        mode = str(raw or "add").strip().lower()
        if mode not in {"add", "override", "max_only"}:
            return "add"
        return mode

    def _sanitize_operator(raw: object) -> str:
        op = str(raw or ">=").strip()
        if op not in {">", ">=", "<", "<=", "==", "!="}:
            return ">="
        return op

    def _sanitize_job_type(raw: object) -> str:
        t = str(raw or "chance").strip().lower()
        if t not in {"chance", "quiz", "timed"}:
            return "chance"
        return t

    def _normalize_req_type(raw: object) -> str:
        req_type = str(raw or "commodity_qty").strip().lower() or "commodity_qty"
        if req_type == "any_single_commodities_qty":
            return "any_single_commodity_qty"
        return req_type

    def _get_config(name: str):
        spec = APP_CONFIG_SPECS[name]
        with _connect() as conn:
            row = conn.execute(
                "SELECT value FROM app_state WHERE key = ?",
                (_config_key(name),),
            ).fetchone()
        raw = spec.default if row is None else row["value"]
        return _normalize_config(name, raw)

    def _set_config(name: str, value):
        normalized = _normalize_config(name, value)
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (_config_key(name), str(normalized)),
            )
            if name in {"MARKET_CLOSE_HOUR", "DISPLAY_TIMEZONE"}:
                conn.execute(
                    "DELETE FROM app_state WHERE key LIKE 'close_event_sent:%'"
                )
                conn.execute(
                    "DELETE FROM app_state WHERE key LIKE 'close_event_armed:%'"
                )
                conn.execute(
                    "DELETE FROM app_state WHERE key LIKE 'rank_income_event:%'"
                )
        return normalized

    def _config_value_for_json(name: str, value):
        # Discord snowflakes exceed JS safe integer range; keep them as strings in JSON.
        if name in {"ANNOUNCEMENT_CHANNEL_ID", "GM_ID"}:
            return str(value)
        return value

    def _create_database_backup_now(prefix: str = "manual") -> str:
        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        backup_path = backup_dir / f"{prefix}_{stamp}.db"
        with _connect() as source_conn, sqlite3.connect(backup_path) as backup_conn:
            source_conn.backup(backup_conn)
        return str(backup_path)

    def _backup_dir() -> Path:
        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir

    def _pick_default_guild_id(conn: sqlite3.Connection) -> int:
        for table in ("companies", "users", "commodities"):
            row = conn.execute(
                f"SELECT guild_id FROM {table} ORDER BY guild_id DESC LIMIT 1"
            ).fetchone()
            if row is not None:
                return int(row["guild_id"])
        return 0

    def _human_size(size: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        for unit in units:
            if value < 1024.0 or unit == units[-1]:
                return f"{value:.1f}{unit}"
            value /= 1024.0
        return f"{size}B"

    def _until_close_text() -> str:
        close_hour = float(_get_config("MARKET_CLOSE_HOUR"))
        tz_name = str(_get_config("DISPLAY_TIMEZONE"))
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc

        now_local = datetime.now(timezone.utc).astimezone(tz)
        close_seconds = max(0, min((24 * 3600) - 1, int(round(close_hour * 3600.0))))
        close_local = (
            now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(seconds=close_seconds)
        )
        if now_local >= close_local:
            close_local = close_local + timedelta(days=1)
        delta = close_local - now_local
        total_seconds = max(0, int(delta.total_seconds()))
        hours, rem = divmod(total_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _until_close_seconds() -> int:
        close_hour = float(_get_config("MARKET_CLOSE_HOUR"))
        tz_name = str(_get_config("DISPLAY_TIMEZONE"))
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
        now_local = datetime.now(timezone.utc).astimezone(tz)
        close_seconds = max(0, min((24 * 3600) - 1, int(round(close_hour * 3600.0))))
        close_local = (
            now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(seconds=close_seconds)
        )
        if now_local >= close_local:
            close_local = close_local + timedelta(days=1)
        return max(0, int((close_local - now_local).total_seconds()))

    def _until_reset_seconds() -> int:
        period = max(1, int(_get_config("TRADING_LIMITS_PERIOD")))
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0
        ticks_remaining = period - (tick % period)
        if ticks_remaining <= 0:
            ticks_remaining = period
        return ticks_remaining * tick_interval

    _ensure_config_defaults()
    _ensure_perk_tables()
    _ensure_jobs_tables()
    _ensure_commodity_tags_table()
    _ensure_commodities_columns()
    _ensure_company_owners_table()

    @app.before_request
    def _auth_gate():
        _cleanup_expired_auth_entries()
        g._new_webadmin_sid = None
        g.webadmin_authenticated = False
        g.webadmin_read_only = True
        if request.path == "/favicon.ico":
            return ("", 204)

        token = (request.args.get("token") or "").strip()
        godtoken_state = (_state_get("webadmin:godtoken") or "").strip()
        godtoken_ok = bool(token) and (
            token == godtoken_env or (godtoken_state and token == godtoken_state)
        )
        if token:
            if godtoken_ok or _token_in_grace(token) or _consume_one_time_token(token):
                sid = _create_session()
                g._new_webadmin_sid = sid
                g.webadmin_authenticated = True
                g.webadmin_read_only = False
                return None

        # Public read-only mode for dashboard + market data endpoints.
        public_paths = {
            "/",
            "/api/stocks",
            "/api/dashboard-stats",
            "/api/app-config/TICK_INTERVAL",
        }
        if request.method == "GET" and (
            request.path in public_paths or request.path.startswith("/media/uploads/")
        ):
            return None
        return render_template_string(AUTH_REQUIRED_HTML), 401

    @app.after_request
    def _set_session_cookie(resp):
        sid = getattr(g, "_new_webadmin_sid", None)
        if sid:
            resp.set_cookie(
                auth_cookie,
                sid,
                max_age=session_ttl_seconds,
                httponly=True,
                samesite="Lax",
                secure=(request.scheme == "https"),
            )
        return resp

    def _history_for(
        conn: sqlite3.Connection,
        symbol: str,
        limit: int | None = 60,
    ) -> list[float]:
        if limit is None:
            rows = conn.execute(
                """
                SELECT price
                FROM price_history
                WHERE symbol = ? COLLATE NOCASE
                ORDER BY tick_index DESC
                """,
                (symbol,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT price
                FROM price_history
                WHERE symbol = ? COLLATE NOCASE
                ORDER BY tick_index DESC
                LIMIT ?
                """,
                (symbol, max(2, limit)),
            ).fetchall()
        return [float(r["price"]) for r in rows[::-1]]

    @app.get("/")
    def dashboard():
        db_access_url = os.getenv("DASHBOARD_DB_ACCESS_URL")
        if not db_access_url:
            host = request.host.split(":", 1)[0]
            db_access_url = f"http://{host}:8081"
        return render_template_string(
            MAIN_HTML,
            db_access_url=db_access_url,
            read_only_mode=bool(getattr(g, "webadmin_read_only", True)),
        )

    @app.get("/media/uploads/<path:filename>")
    def media_uploads(filename: str):
        name = Path(filename).name
        if name != filename or "/" in filename or "\\" in filename:
            return jsonify({"error": "Invalid file path"}), 400
        path = _uploads_dir() / name
        if not path.exists() or not path.is_file():
            return jsonify({"error": "Not found"}), 404
        return send_file(path)

    @app.get("/company/<symbol>")
    def company_page(symbol: str):
        return render_template_string(DETAIL_HTML, symbol=symbol.upper())

    @app.get("/commodity/<name>")
    def commodity_page(name: str):
        return render_template_string(COMMODITY_DETAIL_HTML, name=name)

    @app.get("/player/<int:user_id>")
    def player_page(user_id: int):
        return render_template_string(PLAYER_DETAIL_HTML, user_id=str(user_id))

    @app.get("/app-config/<config_name>")
    def app_config_page(config_name: str):
        return render_template_string(APP_CONFIG_DETAIL_HTML, config_name=config_name)

    @app.get("/perk/<int:perk_id>")
    def perk_page(perk_id: int):
        return render_template_string(PERK_DETAIL_HTML, perk_id=perk_id)

    @app.get("/job/<int:job_id>")
    def job_page(job_id: int):
        return render_template_string(JOB_DETAIL_HTML, job_id=job_id)

    @app.get("/api/stocks")
    def api_stocks():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT guild_id, symbol, name, location, industry, founded_year, description, evaluation,
                       current_price, base_price, slope, drift, liquidity, impact_power, updated_at
                FROM companies
                ORDER BY symbol
                """
            ).fetchall()
            owner_rows = conn.execute(
                """
                SELECT guild_id, symbol, user_id
                FROM company_owners
                ORDER BY guild_id, symbol, user_id
                """
            ).fetchall()
            history_rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        symbol,
                        price,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY tick_index DESC) AS rn
                    FROM price_history
                )
                SELECT symbol, price, rn
                FROM ranked
                WHERE rn <= 40
                ORDER BY symbol, rn DESC
                """
            ).fetchall()

        history_map: dict[str, list[float]] = {}
        for h in history_rows:
            sym = str(h["symbol"])
            history_map.setdefault(sym, []).append(float(h["price"]))
        owners_map: dict[tuple[int, str], list[int]] = {}
        for o in owner_rows:
            key = (int(o["guild_id"]), str(o["symbol"]))
            owners_map.setdefault(key, []).append(int(o["user_id"]))
        stocks: list[dict] = []
        for r in rows:
            row = dict(r)
            owner_ids = owners_map.get((int(r["guild_id"]), str(r["symbol"])), [])
            if not owner_ids:
                legacy_owner = int(row.get("owner_user_id", 0) or 0)
                owner_ids = [legacy_owner] if legacy_owner > 0 else []
            row["owner_user_ids"] = [str(uid) for uid in owner_ids]
            row["owner_user_id"] = str(owner_ids[0]) if owner_ids else "0"
            row["history_prices"] = history_map.get(str(r["symbol"]), [])
            stocks.append(row)
        return jsonify(
            {
                "server_time_utc": datetime.now(timezone.utc).isoformat(),
                "stocks": stocks,
            }
        )

    @app.get("/api/dashboard-stats")
    def api_dashboard_stats():
        with _connect() as conn:
            companies_row = conn.execute("SELECT COUNT(*) AS c FROM companies").fetchone()
            users_row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
            feedback_row = conn.execute("SELECT COUNT(*) AS c FROM feedback").fetchone()
            bank_pending_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM bank_requests
                WHERE status = 'pending'
                """
            ).fetchone()
            guild_id = _pick_default_guild_id(conn)
            sent_row = conn.execute(
                "SELECT value FROM app_state WHERE key = ?",
                (f"close_event_sent:{guild_id}",),
            ).fetchone()
            armed_row = conn.execute(
                "SELECT value FROM app_state WHERE key = ?",
                (f"close_event_armed:{guild_id}",),
            ).fetchone()
        company_count = int(companies_row["c"]) if companies_row is not None else 0
        user_count = int(users_row["c"]) if users_row is not None else 0
        feedback_count = int(feedback_row["c"]) if feedback_row is not None else 0
        bank_pending_count = int(bank_pending_row["c"]) if bank_pending_row is not None else 0
        paused_raw = (_state_get("ticks_paused") or "").strip()
        ticks_paused = paused_raw in {"1", "true", "True", "yes", "on"}
        close_gate_raw = str(sent_row["value"]) if sent_row is not None else ""
        close_gate_armed_raw = str(armed_row["value"]) if armed_row is not None else ""
        close_gate_ready = False
        close_gate_status = "Waiting"
        try:
            tz = ZoneInfo(str(_get_config("DISPLAY_TIMEZONE")))
        except Exception:
            tz = timezone.utc
        close_hour = float(_get_config("MARKET_CLOSE_HOUR"))
        now_local = datetime.now(timezone.utc).astimezone(tz)
        close_seconds = max(0, min((24 * 3600) - 1, int(round(close_hour * 3600.0))))
        close_dt = (
            now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(seconds=close_seconds)
        )
        arm_dt = close_dt - timedelta(minutes=5)
        event_id = f"{close_dt.strftime('%Y-%m-%d')}|{close_hour:.6f}|{str(_get_config('DISPLAY_TIMEZONE'))}"
        if close_gate_raw == event_id or now_local >= close_dt:
            close_gate_ready = False
            close_gate_status = "Sent"
        elif arm_dt <= now_local < close_dt:
            close_gate_ready = True
            close_gate_status = "Ready"
        else:
            close_gate_ready = False
            close_gate_status = "Waiting"
        return jsonify(
            {
                "until_close": _until_close_text(),
                "seconds_until_close": _until_close_seconds(),
                "until_reset": f"{_until_reset_seconds() / 60.0:.2f} min",
                "seconds_until_reset": _until_reset_seconds(),
                "company_count": company_count,
                "user_count": user_count,
                "feedback_count": feedback_count,
                "bank_pending_count": bank_pending_count,
                "ticks_paused": ticks_paused,
                "close_gate_ready": close_gate_ready,
                "close_gate_status": close_gate_status,
                "close_gate_raw": close_gate_raw or "-",
                "close_gate_armed_raw": close_gate_armed_raw or "-",
            }
        )

    @app.get("/api/company/<symbol>")
    def api_company(symbol: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, symbol, name, owner_user_id, location, industry, founded_year, description, evaluation,
                       current_price, base_price, slope, drift, liquidity, impact_power,
                       pending_buy, pending_sell, starting_tick, last_tick, updated_at
                FROM companies
                WHERE symbol = ? COLLATE NOCASE
                """,
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            owner_rows = conn.execute(
                """
                SELECT user_id
                FROM company_owners
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                ORDER BY user_id
                """,
                (int(row["guild_id"]), symbol),
            ).fetchall()
            holder_rows = conn.execute(
                """
                SELECT h.user_id, COALESCE(u.display_name, '') AS display_name, h.shares
                FROM holdings h
                LEFT JOIN users u
                    ON u.guild_id = h.guild_id
                   AND u.user_id = h.user_id
                WHERE h.guild_id = ? AND h.symbol = ? COLLATE NOCASE AND h.shares > 0
                ORDER BY h.shares DESC, h.user_id ASC
                """,
                (int(row["guild_id"]), symbol),
            ).fetchall()
            history = _history_for(conn, symbol, limit=None)
        company = dict(row)
        owner_ids = [int(r["user_id"]) for r in owner_rows]
        if not owner_ids:
            legacy_owner = int(company.get("owner_user_id", 0) or 0)
            owner_ids = [legacy_owner] if legacy_owner > 0 else []
        company["owner_user_ids"] = [str(uid) for uid in owner_ids]
        company["owner_user_id"] = str(owner_ids[0]) if owner_ids else "0"
        shareholders: list[dict[str, object]] = []
        for r in holder_rows:
            uid = str(r["user_id"])
            display_name = str(r["display_name"] or "").strip() or f"User {uid}"
            shares = int(r["shares"] or 0)
            shareholders.append(
                {
                    "user_id": uid,
                    "display_name": display_name,
                    "shares": shares,
                }
            )
        return jsonify(
            {
                "server_time_utc": datetime.now(timezone.utc).isoformat(),
                "company": company,
                "history_prices": history,
                "shareholders": shareholders,
            }
        )

    @app.get("/api/commodities")
    def api_commodities():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT name, price, enabled, rarity, spawn_weight_override, image_url, description,
                       perk_name, perk_description, perk_min_qty, perk_effects_json
                FROM commodities
                ORDER BY name
                """
            ).fetchall()
            tag_rows = conn.execute(
                """
                SELECT commodity_name, tag
                FROM commodity_tags
                ORDER BY commodity_name, tag
                """
            ).fetchall()
        tag_map: dict[str, list[str]] = {}
        for row in tag_rows:
            cname = str(row["commodity_name"])
            tag_map.setdefault(cname.lower(), []).append(str(row["tag"]))
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["tags"] = tag_map.get(str(item.get("name", "")).lower(), [])
            out.append(item)
        return jsonify({"commodities": out})

    @app.get("/api/shop")
    def api_shop():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        bucket, items, available = get_shop_items(guild_id)
        return jsonify(
            {
                "guild_id": guild_id,
                "bucket": bucket,
                "items": items,
                "available": available,
            }
        )

    @app.post("/api/shop/refresh")
    def api_shop_refresh():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        refresh_shop(guild_id)
        bucket, items, available = get_shop_items(guild_id)
        return jsonify({"ok": True, "bucket": bucket, "items": items, "available": available})

    @app.post("/api/shop/availability")
    def api_shop_availability():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        in_stock = bool(data.get("in_stock", True))
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        set_item_availability(guild_id, name, in_stock=in_stock)
        return jsonify({"ok": True, "name": name, "in_stock": in_stock})

    @app.post("/api/shop/swap")
    def api_shop_swap():
        data = request.get_json(silent=True) or {}
        try:
            slot = int(data.get("slot", -1))
        except (TypeError, ValueError):
            return jsonify({"error": "slot must be an integer"}), 400
        new_name = str(data.get("new_name", "")).strip()
        if not new_name:
            return jsonify({"error": "new_name is required"}), 400
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        ok = swap_item(guild_id, slot, new_name)
        if not ok:
            return jsonify({"error": "swap failed (invalid slot/name or duplicate target)"}), 400
        bucket, items, available = get_shop_items(guild_id)
        return jsonify({"ok": True, "bucket": bucket, "items": items, "available": available})

    @app.get("/api/commodity/<name>")
    def api_commodity(name: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, name, price, enabled, rarity, spawn_weight_override, image_url, description,
                       perk_name, perk_description, perk_min_qty, perk_effects_json
                FROM commodities
                WHERE name = ? COLLATE NOCASE
                """,
                (name,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Commodity not found"}), 404
            tag_rows = conn.execute(
                """
                SELECT tag
                FROM commodity_tags
                WHERE commodity_name = ? COLLATE NOCASE
                ORDER BY tag
                """,
                (name,),
            ).fetchall()
            owner_rows = conn.execute(
                """
                SELECT uc.user_id, COALESCE(u.display_name, '') AS display_name, uc.quantity
                FROM user_commodities uc
                LEFT JOIN users u
                  ON u.guild_id = uc.guild_id
                 AND u.user_id = uc.user_id
                WHERE uc.guild_id = ? AND uc.commodity_name = ? COLLATE NOCASE AND uc.quantity > 0
                ORDER BY uc.quantity DESC, uc.user_id ASC
                """,
                (int(row["guild_id"]), name),
            ).fetchall()
        item = dict(row)
        item["tags"] = [str(r["tag"]) for r in tag_rows]
        owners = []
        for r in owner_rows:
            uid = str(r["user_id"])
            display_name = str(r["display_name"] or "").strip() or f"User {uid}"
            owners.append(
                {
                    "user_id": uid,
                    "display_name": display_name,
                    "quantity": int(r["quantity"] or 0),
                }
            )
        return jsonify({"commodity": item, "owners": owners})

    @app.get("/api/players")
    def api_players():
        limit = int(_get_config("TRADING_LIMITS"))
        period = max(1, int(_get_config("TRADING_LIMITS_PERIOD")))
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0
        bucket = tick // period

        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT guild_id, user_id, display_name, rank, bank, networth, owe
                FROM users
                ORDER BY networth DESC, bank DESC, user_id ASC
                """
            ).fetchall()
        active_by_guild: dict[int, set[int]] = {}
        players: list[dict] = []
        for r in rows:
            row = dict(r)
            row["user_id"] = str(row["user_id"])
            guild_id = int(row.get("guild_id", 0))
            if guild_id not in active_by_guild:
                active_by_guild[guild_id] = _active_user_ids_last_day(guild_id)
            user_id = row["user_id"]
            user_id_int = int(user_id)
            row["is_active"] = user_id_int in active_by_guild[guild_id]
            if limit > 0:
                used_raw = _state_get(f"trade_used:{guild_id}:{user_id}:{bucket}")
                try:
                    used = int(float(used_raw)) if used_raw is not None else 0
                except ValueError:
                    used = 0
                row["trade_limit_enabled"] = True
                row["trade_limit_limit"] = limit
                row["trade_limit_remaining"] = max(0, limit - used)
            else:
                row["trade_limit_enabled"] = False
                row["trade_limit_limit"] = 0
                row["trade_limit_remaining"] = 0
            players.append(row)
        return jsonify({"players": players})

    @app.get("/api/perks")
    def api_perks():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.id,
                    p.guild_id,
                    p.name,
                    p.description,
                    p.enabled,
                    p.priority,
                    p.stack_mode,
                    p.max_stacks,
                    (
                        SELECT COUNT(*)
                        FROM perk_requirements pr
                        WHERE pr.perk_id = p.id
                    ) AS requirements_count,
                    (
                        SELECT COUNT(*)
                        FROM perk_effects pe
                        WHERE pe.perk_id = p.id
                    ) AS effects_count
                FROM perks p
                ORDER BY p.priority ASC, p.id ASC
                """
            ).fetchall()
        return jsonify({"perks": [dict(r) for r in rows]})

    @app.get("/api/perk/<int:perk_id>")
    def api_perk(perk_id: int):
        with _connect() as conn:
            perk_row = conn.execute(
                """
                SELECT id, guild_id, name, description, enabled, priority, stack_mode, max_stacks
                FROM perks
                WHERE id = ?
                """,
                (perk_id,),
            ).fetchone()
            if perk_row is None:
                return jsonify({"error": "Perk not found"}), 404
            req_rows = conn.execute(
                """
                SELECT id, perk_id, group_id, req_type, commodity_name, operator, value
                FROM perk_requirements
                WHERE perk_id = ?
                ORDER BY group_id ASC, id ASC
                """,
                (perk_id,),
            ).fetchall()
            effect_rows = conn.execute(
                """
                SELECT id, perk_id, effect_type, target_stat, value_mode, value, scale_source, scale_key, scale_factor, cap
                FROM perk_effects
                WHERE perk_id = ?
                ORDER BY id ASC
                """,
                (perk_id,),
            ).fetchall()
        return jsonify(
            {
                "perk": dict(perk_row),
                "requirements": [dict(r) for r in req_rows],
                "effects": [dict(r) for r in effect_rows],
            }
        )

    @app.get("/api/jobs")
    def api_jobs():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            rows = conn.execute(
                """
                SELECT id, guild_id, name, description, job_type, enabled, cooldown_ticks,
                       reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                       quiz_question, quiz_answer, config_json, updated_at
                FROM jobs
                WHERE guild_id = ?
                ORDER BY id ASC
                """,
                (guild_id,),
            ).fetchall()
        jobs = [dict(r) for r in rows]
        rotation = get_active_jobs(guild_id, limit=5)
        available = [
            {"id": int(r.get("id", 0)), "name": str(r.get("name", ""))}
            for r in jobs
            if int(r.get("enabled", 0)) == 1
        ]
        return jsonify(
            {
                "jobs": jobs,
                "rotation": rotation,
                "available": available,
            }
        )

    @app.get("/api/jobs/rotation")
    def api_jobs_rotation():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            rows = conn.execute(
                """
                SELECT id, name, enabled
                FROM jobs
                WHERE guild_id = ?
                ORDER BY id ASC
                """,
                (guild_id,),
            ).fetchall()
        available = [{"id": int(r["id"]), "name": str(r["name"])} for r in rows if int(r["enabled"]) == 1]
        return jsonify({"rotation": get_active_jobs(guild_id, limit=5), "available": available})

    @app.post("/api/jobs/rotation/refresh")
    def api_jobs_rotation_refresh():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        rotation = refresh_jobs_rotation(guild_id, limit=5)
        return jsonify({"ok": True, "rotation": rotation})

    @app.post("/api/jobs/rotation/swap")
    def api_jobs_rotation_swap():
        data = request.get_json(silent=True) or {}
        try:
            slot = int(data.get("slot", -1))
            new_job_id = int(data.get("new_job_id", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "slot and new_job_id must be integers"}), 400
        if slot < 0:
            return jsonify({"error": "slot must be >= 0"}), 400
        if new_job_id <= 0:
            return jsonify({"error": "new_job_id must be > 0"}), 400
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        ok = swap_active_job(guild_id, slot, new_job_id, limit=5)
        if not ok:
            return jsonify({"error": "swap failed (invalid slot/job or duplicate target)"}), 400
        return jsonify({"ok": True, "rotation": get_active_jobs(guild_id, limit=5)})

    @app.post("/api/job")
    def api_job_create():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        job_type = _sanitize_job_type(data.get("job_type", "chance"))
        description = str(data.get("description", "")).strip()
        quiz_question = str(data.get("quiz_question", "")).strip()
        quiz_answer = str(data.get("quiz_answer", "")).strip()
        raw_config = data.get("config_json", {})
        if isinstance(raw_config, dict):
            cfg_obj = raw_config
        else:
            try:
                cfg_obj = json.loads(str(raw_config or "{}"))
            except json.JSONDecodeError:
                return jsonify({"error": "config_json must be valid JSON"}), 400
            if not isinstance(cfg_obj, dict):
                return jsonify({"error": "config_json must be a JSON object"}), 400
        config_json = json.dumps(cfg_obj, separators=(",", ":"))
        try:
            enabled = 1 if int(data.get("enabled", 1)) > 0 else 0
            reward_min = max(0.0, float(data.get("reward_min", 0.0)))
            reward_max = max(0.0, float(data.get("reward_max", reward_min)))
            duration_minutes = max(0.0, float(data.get("duration_minutes", data.get("duration_ticks", 0))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid numeric field"}), 400
        if reward_max < reward_min:
            reward_min, reward_max = reward_max, reward_min
        if job_type == "quiz" and not quiz_question:
            return jsonify({"error": "quiz jobs require quiz_question"}), 400
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            now = datetime.now(timezone.utc).isoformat()
            try:
                cur = conn.execute(
                    """
                    INSERT INTO jobs (
                        guild_id, name, description, job_type, enabled, cooldown_ticks,
                        reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                        quiz_question, quiz_answer, config_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id, name, description, job_type, enabled, 0,
                        reward_min, reward_max, 1.0, int(duration_minutes), duration_minutes,
                        quiz_question, quiz_answer, config_json, now,
                    ),
                )
            except sqlite3.IntegrityError:
                return jsonify({"error": "Job with this name already exists"}), 400
        return jsonify({"ok": True, "id": int(cur.lastrowid)})

    @app.get("/api/job/<int:job_id>")
    def api_job_get(job_id: int):
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            row = conn.execute(
                """
                SELECT id, guild_id, name, description, job_type, enabled, cooldown_ticks,
                       reward_min, reward_max, success_rate, duration_ticks, duration_minutes,
                       quiz_question, quiz_answer, config_json, updated_at
                FROM jobs
                WHERE guild_id = ? AND id = ?
                """,
                (guild_id, job_id),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Job not found"}), 404
        return jsonify({"job": dict(row)})

    @app.post("/api/job/<int:job_id>/update")
    def api_job_update(job_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "name" in data:
            name = str(data.get("name", "")).strip()
            if not name:
                return jsonify({"error": "name cannot be empty"}), 400
            updates["name"] = name
        if "description" in data:
            updates["description"] = str(data.get("description", "")).strip()
        if "job_type" in data:
            updates["job_type"] = _sanitize_job_type(data.get("job_type", "chance"))
        if "enabled" in data:
            updates["enabled"] = 1 if int(data.get("enabled", 1)) > 0 else 0
        if "duration_minutes" in data or "duration_ticks" in data:
            try:
                updates["duration_minutes"] = max(
                    0.0,
                    float(data.get("duration_minutes", data.get("duration_ticks", 0))),
                )
                updates["duration_ticks"] = max(0, int(float(updates["duration_minutes"])))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for duration_minutes"}), 400
        numeric_float_fields = {"reward_min", "reward_max"}
        for key in numeric_float_fields:
            if key in data:
                try:
                    updates[key] = max(0.0, float(data.get(key, 0.0)))
                except (TypeError, ValueError):
                    return jsonify({"error": f"Invalid value for {key}"}), 400
        if "quiz_question" in data:
            updates["quiz_question"] = str(data.get("quiz_question", "")).strip()
        if "quiz_answer" in data:
            updates["quiz_answer"] = str(data.get("quiz_answer", "")).strip()
        if "config_json" in data:
            raw_config = data.get("config_json", {})
            if isinstance(raw_config, dict):
                cfg_obj = raw_config
            else:
                try:
                    cfg_obj = json.loads(str(raw_config or "{}"))
                except json.JSONDecodeError:
                    return jsonify({"error": "config_json must be valid JSON"}), 400
                if not isinstance(cfg_obj, dict):
                    return jsonify({"error": "config_json must be a JSON object"}), 400
            updates["config_json"] = json.dumps(cfg_obj, separators=(",", ":"))
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400
        updates["updated_at"] = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            if "reward_min" in updates or "reward_max" in updates:
                row = conn.execute(
                    """
                    SELECT reward_min, reward_max
                    FROM jobs
                    WHERE guild_id = ? AND id = ?
                    """,
                    (guild_id, job_id),
                ).fetchone()
                if row is None:
                    return jsonify({"error": "Job not found"}), 404
                rmin = float(updates.get("reward_min", row["reward_min"]))
                rmax = float(updates.get("reward_max", row["reward_max"]))
                if rmax < rmin:
                    updates["reward_min"] = rmax
                    updates["reward_max"] = rmin
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            vals = list(updates.values()) + [guild_id, job_id]
            try:
                cur = conn.execute(
                    f"""
                    UPDATE jobs
                    SET {set_clause}
                    WHERE guild_id = ? AND id = ?
                    """,
                    vals,
                )
            except sqlite3.IntegrityError:
                return jsonify({"error": "Job name already exists"}), 400
            if cur.rowcount <= 0:
                return jsonify({"error": "Job not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/job/<int:job_id>")
    def api_job_delete(job_id: int):
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            cur = conn.execute(
                """
                DELETE FROM jobs
                WHERE guild_id = ? AND id = ?
                """,
                (guild_id, job_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Job not found"}), 404
        return jsonify({"ok": True})

    @app.get("/api/action-history")
    def api_action_history():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    ah.created_at,
                    ah.user_id,
                    COALESCE(u.display_name, '') AS display_name,
                    ah.action_type,
                    ah.target_type,
                    ah.target_symbol,
                    ah.quantity,
                    ah.unit_price,
                    ah.total_amount,
                    ah.details
                FROM action_history ah
                LEFT JOIN users u
                  ON u.guild_id = ah.guild_id
                 AND u.user_id = ah.user_id
                ORDER BY ah.id DESC
                LIMIT 400
                """
            ).fetchall()
        return jsonify({"actions": [dict(r) for r in rows]})

    @app.delete("/api/action-history")
    def api_action_history_delete():
        with _connect() as conn:
            cur = conn.execute("DELETE FROM action_history")
        return jsonify({"ok": True, "deleted": int(cur.rowcount)})

    @app.get("/api/feedback")
    def api_feedback():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT id, guild_id, message, created_at
                FROM feedback
                ORDER BY id DESC
                LIMIT 500
                """
            ).fetchall()
        return jsonify({"feedback": [dict(r) for r in rows]})

    @app.delete("/api/feedback/<int:feedback_id>")
    def api_feedback_delete(feedback_id: int):
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM feedback
                WHERE id = ?
                """,
                (feedback_id,),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Feedback not found"}), 404
        return jsonify({"ok": True, "deleted": feedback_id})

    @app.get("/api/player/<int:user_id>")
    def api_player(user_id: int):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, user_id, display_name, joined_at, rank, bank, networth, owe
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
        player = dict(row)
        guild_id = int(player.get("guild_id", 0))
        limit = int(_get_config("TRADING_LIMITS"))
        period = int(_get_config("TRADING_LIMITS_PERIOD"))
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0

        if limit > 0 and period > 0:
            bucket = tick // period
            used_raw = _state_get(f"trade_used:{guild_id}:{user_id}:{bucket}")
            try:
                used = int(float(used_raw)) if used_raw is not None else 0
            except ValueError:
                used = 0
            remaining = max(0, limit - used)
            player["trade_limit_enabled"] = True
            player["trade_limit_limit"] = limit
            player["trade_limit_used"] = used
            player["trade_limit_remaining"] = remaining
            player["trade_limit_window_minutes"] = (period * tick_interval) / 60.0
        else:
            player["trade_limit_enabled"] = False
            player["trade_limit_limit"] = limit
            player["trade_limit_used"] = 0
            player["trade_limit_remaining"] = 0
            player["trade_limit_window_minutes"] = 0.0

        return jsonify({"player": player})

    @app.get("/api/player/<int:user_id>/bank-history")
    def api_player_bank_history(user_id: int):
        with _connect() as conn:
            user_row = conn.execute(
                """
                SELECT guild_id, user_id
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(user_row["guild_id"])
            rows = conn.execute(
                """
                SELECT
                    ah.id,
                    ah.created_at,
                    ah.action_type,
                    ah.target_type,
                    ah.target_symbol,
                    ah.quantity,
                    ah.unit_price,
                    ah.total_amount,
                    ah.details
                FROM action_history ah
                WHERE ah.guild_id = ? AND ah.user_id = ?
                ORDER BY ah.id DESC
                LIMIT 200
                """,
                (guild_id, user_id),
            ).fetchall()

        debit_actions = {"buy", "loan_repayment"}
        credit_actions = {"sell", "pawn", "loan_approved", "owner_payout"}

        out: list[dict] = []
        for row in rows:
            item = dict(row)
            action = str(item.get("action_type") or "").strip().lower()
            amount = float(item.get("total_amount") or 0.0)
            if action == "job":
                delta = amount
            elif action in debit_actions:
                delta = -abs(amount)
            elif action in credit_actions:
                delta = abs(amount)
            else:
                delta = 0.0
            item["delta"] = delta
            out.append(item)

        return jsonify({"history": out})

    @app.get("/api/player/<int:user_id>/commodities")
    def api_player_commodities(user_id: int):
        with _connect() as conn:
            user_row = conn.execute(
                """
                SELECT guild_id, user_id
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(user_row["guild_id"])
            inv_rows = conn.execute(
                """
                SELECT commodity_name AS name, quantity
                FROM user_commodities
                WHERE guild_id = ? AND user_id = ? AND quantity > 0
                ORDER BY commodity_name ASC
                """,
                (guild_id, user_id),
            ).fetchall()
            all_rows = conn.execute(
                """
                SELECT name
                FROM commodities
                WHERE guild_id = ?
                ORDER BY name ASC
                """,
                (guild_id,),
            ).fetchall()
        return jsonify(
            {
                "inventory": [dict(r) for r in inv_rows],
                "available": [str(r["name"]) for r in all_rows],
            }
        )

    @app.post("/api/player/<int:user_id>/commodities/set")
    def api_player_set_commodity_qty(user_id: int):
        data = request.get_json(silent=True) or {}
        commodity_name = str(data.get("commodity_name", "")).strip()
        if not commodity_name:
            return jsonify({"error": "commodity_name is required"}), 400
        try:
            quantity = max(0, int(float(data.get("quantity", 0))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid quantity"}), 400

        with _connect() as conn:
            user_row = conn.execute(
                """
                SELECT guild_id, user_id
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(user_row["guild_id"])
            exists = conn.execute(
                """
                SELECT 1
                FROM commodities
                WHERE guild_id = ? AND name = ? COLLATE NOCASE
                """,
                (guild_id, commodity_name),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Commodity not found"}), 404

            if quantity <= 0:
                conn.execute(
                    """
                    DELETE FROM user_commodities
                    WHERE guild_id = ? AND user_id = ? AND commodity_name = ? COLLATE NOCASE
                    """,
                    (guild_id, user_id, commodity_name),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, commodity_name)
                    DO UPDATE SET quantity = excluded.quantity
                    """,
                    (guild_id, user_id, commodity_name, quantity),
                )

            # Keep networth in sync with inventory edits.
            networth_row = conn.execute(
                """
                SELECT COALESCE(SUM(uc.quantity * c.price), 0.0) AS total
                FROM user_commodities uc
                JOIN commodities c
                  ON c.guild_id = uc.guild_id
                 AND c.name = uc.commodity_name
                WHERE uc.guild_id = ? AND uc.user_id = ?
                """,
                (guild_id, user_id),
            ).fetchone()
            total = float(networth_row["total"]) if networth_row is not None else 0.0
            conn.execute(
                """
                UPDATE users
                SET networth = ?
                WHERE guild_id = ? AND user_id = ?
                """,
                (total, guild_id, user_id),
            )

        return jsonify({"ok": True, "commodity_name": commodity_name, "quantity": quantity})

    @app.get("/api/perk-preview")
    def api_perk_preview():
        user_raw = (request.args.get("user_id") or "").strip()
        if not user_raw:
            return jsonify({"error": "user_id is required"}), 400
        try:
            user_id = int(user_raw)
        except ValueError:
            return jsonify({"error": "Invalid user_id"}), 400

        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, user_id, display_name, rank
                FROM users
                WHERE user_id = ?
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(row["guild_id"])
            rank = str(row["rank"] or DEFAULT_RANK)
            display_name = str(row["display_name"] or f"User {user_id}")

        lookup = {k.lower(): float(v) for k, v in RANK_INCOME.items()}
        base_income = lookup.get(rank.lower(), float(RANK_INCOME.get(DEFAULT_RANK, 0.0)))
        base_trade_limits = int(_get_config("TRADING_LIMITS"))
        base_commodities_limit = int(_get_config("COMMODITIES_LIMIT"))
        result = evaluate_user_perks(
            guild_id=guild_id,
            user_id=user_id,
            base_income=base_income,
            base_trade_limits=base_trade_limits,
            base_networth=None,
            base_commodities_limit=base_commodities_limit,
            base_job_slots=1,
            base_timed_duration=0.0,
            base_chance_roll_bonus=0.0,
        )
        matched = list(result.get("matched_perks", []))
        return jsonify(
            {
                "user_id": str(user_id),
                "display_name": display_name,
                "rank": rank,
                "base_income": float(result["base"]["income"]),
                "final_income": float(result["final"]["income"]),
                "base_trade_limits": float(result["base"]["trade_limits"]),
                "final_trade_limits": float(result["final"]["trade_limits"]),
                "base_networth": float(result["base"]["networth"]),
                "final_networth": float(result["final"]["networth"]),
                "base_commodities_limit": float(result["base"]["commodities_limit"]),
                "final_commodities_limit": float(result["final"]["commodities_limit"]),
                "base_job_slots": float(result["base"]["job_slots"]),
                "final_job_slots": float(result["final"]["job_slots"]),
                "base_timed_duration": float(result["base"]["timed_duration"]),
                "final_timed_duration": float(result["final"]["timed_duration"]),
                "base_chance_roll_bonus": float(result["base"]["chance_roll_bonus"]),
                "final_chance_roll_bonus": float(result["final"]["chance_roll_bonus"]),
                "matched_perks": matched,
            }
        )

    @app.get("/api/app-configs")
    def api_app_configs():
        configs = []
        for name, spec in APP_CONFIG_SPECS.items():
            value = _get_config(name)
            configs.append(
                {
                    "name": name,
                    "value": _config_value_for_json(name, value),
                    "default": _normalize_config(name, spec.default),
                    "type": spec.cast.__name__,
                    "description": spec.description,
                }
            )
        return jsonify({"configs": configs})

    @app.get("/api/app-config/<config_name>")
    def api_app_config(config_name: str):
        if config_name not in APP_CONFIG_SPECS:
            return jsonify({"error": "Unknown app config"}), 404
        spec = APP_CONFIG_SPECS[config_name]
        value = _get_config(config_name)
        return jsonify(
            {
                "config": {
                    "name": config_name,
                    "value": _config_value_for_json(config_name, value),
                    "default": _normalize_config(config_name, spec.default),
                    "type": spec.cast.__name__,
                    "description": spec.description,
                }
            }
        )

    @app.get("/api/close-announcement")
    def api_close_announcement_get():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        markdown = _state_get(f"close_announcement_md:{guild_id}") or ""
        return jsonify({"guild_id": guild_id, "markdown": markdown})

    @app.post("/api/close-announcement")
    def api_close_announcement_set():
        data = request.get_json(silent=True) or {}
        markdown = str(data.get("markdown", ""))
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        _state_set(f"close_announcement_md:{guild_id}", markdown)
        return jsonify({"ok": True, "guild_id": guild_id, "markdown": markdown})

    @app.get("/api/close-news")
    def api_close_news():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            rows = conn.execute(
                """
                SELECT id, guild_id, title, body, image_url, sort_order, enabled, updated_at
                FROM close_news
                WHERE guild_id = ?
                ORDER BY sort_order ASC, id ASC
                """,
                (guild_id,),
            ).fetchall()
        return jsonify({"guild_id": guild_id, "news": [dict(r) for r in rows]})

    @app.post("/api/close-news")
    def api_close_news_create():
        data = request.get_json(silent=True) or {}
        title = str(data.get("title", "")).strip()
        body = str(data.get("body", ""))
        try:
            sort_order = int(data.get("sort_order", 0))
            enabled = 1 if int(data.get("enabled", 1)) != 0 else 0
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid sort_order/enabled"}), 400
        if not title:
            return jsonify({"error": "title is required"}), 400
        now_iso = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            try:
                image_url, image_status = _normalize_image_url_with_status(conn, data.get("image_url", ""))
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
            cur = conn.execute(
                """
                INSERT INTO close_news (guild_id, title, body, image_url, sort_order, enabled, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (guild_id, title, body, image_url, sort_order, enabled, now_iso),
            )
            news_id = int(cur.lastrowid)
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "id": news_id, "image_status": image_status, "image_url": image_url})

    @app.post("/api/close-news/<int:news_id>")
    def api_close_news_update(news_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "title" in data:
            title = str(data.get("title", "")).strip()
            if not title:
                return jsonify({"error": "title cannot be empty"}), 400
            updates["title"] = title
        if "body" in data:
            updates["body"] = str(data.get("body", ""))
        if "image_url" in data:
            updates["image_url"] = data.get("image_url", "")
        if "sort_order" in data:
            try:
                updates["sort_order"] = int(data.get("sort_order", 0))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid sort_order"}), 400
        if "enabled" in data:
            try:
                updates["enabled"] = 1 if int(data.get("enabled", 1)) != 0 else 0
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid enabled"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400

        updates["updated_at"] = datetime.now(timezone.utc).isoformat()
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.append(news_id)
        image_status = "unchanged"
        with _connect() as conn:
            if "image_url" in updates:
                try:
                    updates["image_url"], image_status = _normalize_image_url_with_status(conn, updates["image_url"])
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400
                values = list(updates.values())
                values.append(news_id)
            cur = conn.execute(
                f"UPDATE close_news SET {set_clause} WHERE id = ?",
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "News item not found"}), 404
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "image_status": image_status, "image_url": str(updates.get("image_url", ""))})

    @app.delete("/api/close-news/<int:news_id>")
    def api_close_news_delete(news_id: int):
        with _connect() as conn:
            cur = conn.execute(
                "DELETE FROM close_news WHERE id = ?",
                (news_id,),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "News item not found"}), 404
            _gc_uploaded_images(conn)
        return jsonify({"ok": True})

    @app.post("/api/company/<symbol>/adjust")
    def api_company_adjust(symbol: str):
        data = request.get_json(silent=True) or {}
        field = str(data.get("field", "")).strip()
        if field not in {"base_price", "slope"}:
            return jsonify({"error": "Only base_price and slope are adjustable here."}), 400
        try:
            delta = float(data.get("delta", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid delta"}), 400

        with _connect() as conn:
            row = conn.execute(
                f"SELECT {field} FROM companies WHERE symbol = ? COLLATE NOCASE",
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            current = float(row[field])
            next_value = current + delta
            if field == "base_price":
                next_value = max(0.01, next_value)
            conn.execute(
                f"UPDATE companies SET {field} = ?, updated_at = ? WHERE symbol = ? COLLATE NOCASE",
                (next_value, datetime.now(timezone.utc).isoformat(), symbol),
            )
        return jsonify({"ok": True, field: next_value})

    @app.post("/api/company")
    def api_company_create():
        data = request.get_json(silent=True) or {}
        symbol = str(data.get("symbol", "")).strip().upper()
        name = str(data.get("name", "")).strip()
        if not symbol or not name:
            return jsonify({"error": "symbol and name are required"}), 400
        try:
            base_price = max(0.01, float(data.get("base_price", 1.0)))
            slope = float(data.get("slope", 0.0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid base_price/slope"}), 400
        owner_user_ids = _parse_owner_user_ids(data.get("owner_user_ids", data.get("owner_user_id", "")))
        canonical_owner = owner_user_ids[0] if owner_user_ids else 0

        now_iso = datetime.now(timezone.utc).isoformat()
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            existing = conn.execute(
                """
                SELECT 1
                FROM companies
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, symbol),
            ).fetchone()
            if existing is not None:
                return jsonify({"error": f"Company `{symbol}` already exists"}), 409
            conn.execute(
                """
                INSERT INTO companies (
                    guild_id, symbol, name, owner_user_id, location, industry, founded_year, description, evaluation,
                    base_price, slope, drift, liquidity, impact_power, pending_buy, pending_sell,
                    starting_tick, current_price, last_tick, updated_at
                )
                VALUES (?, ?, ?, ?, '', '', 2000, '', '', ?, ?, 0.0, 100.0, 1.0, 0.0, 0.0, ?, ?, ?, ?)
                """,
                (guild_id, symbol, name, canonical_owner, base_price, slope, tick, base_price, tick, now_iso),
            )
            _set_company_owners(conn, guild_id, symbol, owner_user_ids)
            conn.execute(
                """
                INSERT OR REPLACE INTO price_history (guild_id, symbol, tick_index, ts, price)
                VALUES (?, ?, ?, ?, ?)
                """,
                (guild_id, symbol, tick, now_iso, base_price),
            )
        return jsonify({"ok": True, "symbol": symbol})

    @app.post("/api/company/<symbol>/update")
    def api_company_update(symbol: str):
        data = request.get_json(silent=True) or {}
        allowed = {
            "name": str,
            "owner_user_id": int,
            "owner_user_ids": str,
            "location": str,
            "industry": str,
            "founded_year": int,
            "description": str,
            "evaluation": str,
            "current_price": float,
            "base_price": float,
            "slope": float,
            "drift": float,
            "liquidity": float,
            "impact_power": float,
            "pending_buy": float,
            "pending_sell": float,
            "starting_tick": int,
            "last_tick": int,
        }
        updates: dict[str, object] = {}
        for key, caster in allowed.items():
            if key not in data:
                continue
            if key == "owner_user_ids":
                continue
            raw = data.get(key)
            try:
                value = caster(raw) if caster is not str else str(raw)
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid value for {key}"}), 400
            updates[key] = value
        owner_user_ids_raw = data.get("owner_user_ids")
        owner_user_ids = None
        if owner_user_ids_raw is not None:
            owner_user_ids = _parse_owner_user_ids(owner_user_ids_raw)

        if not updates and owner_user_ids is None:
            return jsonify({"error": "No valid fields provided"}), 400

        if "base_price" in updates:
            updates["base_price"] = max(0.01, float(updates["base_price"]))
        if "current_price" in updates:
            updates["current_price"] = max(0.01, float(updates["current_price"]))
        if "liquidity" in updates:
            updates["liquidity"] = max(1.0, float(updates["liquidity"]))
        if "impact_power" in updates:
            updates["impact_power"] = max(0.1, float(updates["impact_power"]))
        if "founded_year" in updates:
            updates["founded_year"] = max(0, int(updates["founded_year"]))
        if "owner_user_id" in updates:
            updates["owner_user_id"] = max(0, int(updates["owner_user_id"]))
            if owner_user_ids is None:
                owner_user_ids = _parse_owner_user_ids(updates["owner_user_id"])
        if "starting_tick" in updates:
            updates["starting_tick"] = max(0, int(updates["starting_tick"]))
        if "last_tick" in updates:
            updates["last_tick"] = max(0, int(updates["last_tick"]))

        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.extend([datetime.now(timezone.utc).isoformat(), symbol])

        with _connect() as conn:
            row = conn.execute(
                "SELECT guild_id FROM companies WHERE symbol = ? COLLATE NOCASE",
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            guild_id = int(row["guild_id"])
            if set_clause:
                conn.execute(
                    f"UPDATE companies SET {set_clause}, updated_at = ? WHERE symbol = ? COLLATE NOCASE",
                    values,
                )
            elif owner_user_ids is not None:
                conn.execute(
                    "UPDATE companies SET updated_at = ? WHERE symbol = ? COLLATE NOCASE",
                    (datetime.now(timezone.utc).isoformat(), symbol),
                )
            if owner_user_ids is not None:
                _set_company_owners(conn, guild_id, symbol, owner_user_ids)
                conn.execute(
                    "UPDATE companies SET owner_user_id = ? WHERE symbol = ? COLLATE NOCASE",
                    (owner_user_ids[0] if owner_user_ids else 0, symbol),
                )
        return jsonify({"ok": True})

    @app.delete("/api/company/<symbol>")
    def api_company_delete(symbol: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, symbol
                FROM companies
                WHERE symbol = ? COLLATE NOCASE
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            guild_id = int(row["guild_id"])
            real_symbol = str(row["symbol"])

            conn.execute(
                """
                DELETE FROM companies
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
            conn.execute(
                """
                DELETE FROM price_history
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
            conn.execute(
                """
                DELETE FROM daily_close
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
            conn.execute(
                """
                DELETE FROM holdings
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
        return jsonify({"ok": True, "deleted": real_symbol})

    @app.post("/api/commodity/<name>/update")
    def api_commodity_update(name: str):
        data = request.get_json(silent=True) or {}
        allowed = {
            "name": str,
            "price": float,
            "enabled": int,
            "rarity": str,
            "spawn_weight_override": float,
            "image_url": str,
            "description": str,
            "perk_name": str,
            "perk_description": str,
            "perk_min_qty": int,
            "perk_effects_json": str,
        }
        updates: dict[str, object] = {}
        for key, caster in allowed.items():
            if key not in data:
                continue
            raw = data.get(key)
            try:
                value = caster(raw) if caster is not str else str(raw)
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid value for {key}"}), 400
            updates[key] = value
        tags_in_payload = "tags" in data
        if not updates and not tags_in_payload:
            return jsonify({"error": "No valid fields provided"}), 400
        if "price" in updates:
            updates["price"] = max(0.01, float(updates["price"]))
        if "enabled" in updates:
            updates["enabled"] = 1 if int(updates["enabled"]) else 0
        if "spawn_weight_override" in updates:
            updates["spawn_weight_override"] = max(0.0, float(updates["spawn_weight_override"]))
        if "perk_min_qty" in updates:
            updates["perk_min_qty"] = max(1, int(updates["perk_min_qty"]))
        if "perk_effects_json" in updates:
            raw_json = str(updates["perk_effects_json"]).strip() or "[]"
            try:
                parsed = json.loads(raw_json)
                if not isinstance(parsed, (dict, list)):
                    return jsonify({"error": "perk_effects_json must be an object or array"}), 400
            except json.JSONDecodeError:
                return jsonify({"error": "Invalid perk_effects_json"}), 400
            updates["perk_effects_json"] = raw_json
        tags = _parse_tags(data.get("tags", ""))

        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM commodities WHERE name = ? COLLATE NOCASE",
                (name,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Commodity not found"}), 404
            guild_row = conn.execute(
                """
                SELECT guild_id, name
                FROM commodities
                WHERE name = ? COLLATE NOCASE
                """,
                (name,),
            ).fetchone()
            if guild_row is None:
                return jsonify({"error": "Commodity not found"}), 404
            guild_id = int(guild_row["guild_id"])
            old_name = str(guild_row["name"])
            next_name = str(updates.get("name", old_name))
            if "image_url" in updates:
                try:
                    updates["image_url"] = _normalize_image_url_for_storage(conn, updates.get("image_url", ""))
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400
            if updates:
                set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                values = list(updates.values())
                values.append(name)
                conn.execute(
                    f"UPDATE commodities SET {set_clause} WHERE name = ? COLLATE NOCASE",
                    values,
                )
            conn.execute(
                """
                DELETE FROM commodity_tags
                WHERE guild_id = ? AND commodity_name = ? COLLATE NOCASE
                """,
                (guild_id, old_name),
            )
            for tag in tags:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO commodity_tags (guild_id, commodity_name, tag)
                    VALUES (?, ?, ?)
                    """,
                    (guild_id, next_name, tag),
                )
            _gc_uploaded_images(conn)
        return jsonify({"ok": True})

    @app.post("/api/commodity")
    def api_commodity_create():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        try:
            price = max(0.01, float(data.get("price", 1.0)))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid price"}), 400
        rarity = str(data.get("rarity", "common")).strip().lower() or "common"
        try:
            enabled = 1 if int(data.get("enabled", 1) or 0) else 0
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid enabled value"}), 400
        try:
            spawn_weight_override = max(0.0, float(data.get("spawn_weight_override", 0.0)))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid spawn_weight_override"}), 400
        description = str(data.get("description", "")).strip()
        perk_name = str(data.get("perk_name", "")).strip()
        perk_description = str(data.get("perk_description", "")).strip()
        try:
            perk_min_qty = max(1, int(float(data.get("perk_min_qty", 1))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid perk_min_qty"}), 400
        perk_effects_json = str(data.get("perk_effects_json", "")).strip()
        if not perk_effects_json:
            perk_effects_json = "[]"
        try:
            parsed_perk = json.loads(perk_effects_json)
            if not isinstance(parsed_perk, (dict, list)):
                return jsonify({"error": "perk_effects_json must be an object or array"}), 400
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid perk_effects_json"}), 400
        tags = _parse_tags(data.get("tags", ""))

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            try:
                image_url = _normalize_image_url_for_storage(conn, data.get("image_url", ""))
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
            existing = conn.execute(
                """
                SELECT 1
                FROM commodities
                WHERE guild_id = ? AND name = ? COLLATE NOCASE
                """,
                (guild_id, name),
            ).fetchone()
            if existing is not None:
                return jsonify({"error": f"Commodity `{name}` already exists"}), 409
            conn.execute(
                """
                INSERT INTO commodities (
                    guild_id, name, price, enabled, rarity, spawn_weight_override, image_url, description,
                    perk_name, perk_description, perk_min_qty, perk_effects_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    guild_id, name, price, enabled, rarity, spawn_weight_override, image_url, description,
                    perk_name, perk_description, perk_min_qty, perk_effects_json,
                ),
            )
            for tag in tags:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO commodity_tags (guild_id, commodity_name, tag)
                    VALUES (?, ?, ?)
                    """,
                    (guild_id, name, tag),
                )
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "name": name})

    @app.post("/api/commodity/<name>/delete")
    def api_commodity_delete(name: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, name
                FROM commodities
                WHERE name = ? COLLATE NOCASE
                """,
                (name,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Commodity not found"}), 404
            guild_id = int(row["guild_id"])
            real_name = str(row["name"])
            conn.execute(
                """
                DELETE FROM commodities
                WHERE guild_id = ? AND name = ? COLLATE NOCASE
                """,
                (guild_id, real_name),
            )
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "deleted": real_name})

    @app.post("/api/player/<int:user_id>/update")
    def api_player_update(user_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "bank" in data:
            try:
                updates["bank"] = max(0.0, float(data.get("bank")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for bank"}), 400
        if "networth" in data:
            try:
                updates["networth"] = max(0.0, float(data.get("networth")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for networth"}), 400
        if "owe" in data:
            try:
                updates["owe"] = max(0.0, float(data.get("owe")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for owe"}), 400
        if "rank" in data:
            updates["rank"] = str(data.get("rank", "")).strip()
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400

        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.append(user_id)
        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            conn.execute(
                f"UPDATE users SET {set_clause} WHERE user_id = ?",
                values,
            )
        return jsonify({"ok": True})

    @app.post("/api/player/<int:user_id>/reset-trade-usage")
    def api_player_reset_trade_usage(user_id: int):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(row["guild_id"])
            cur = conn.execute(
                """
                DELETE FROM app_state
                WHERE key LIKE ?
                """,
                (f"trade_used:{guild_id}:{user_id}:%",),
            )
        return jsonify({"ok": True, "cleared": int(cur.rowcount)})

    @app.post("/api/perk")
    def api_perk_create():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        description = str(data.get("description", "")).strip()
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            cur = conn.execute(
                """
                INSERT INTO perks (guild_id, name, description, enabled, priority, stack_mode, max_stacks)
                VALUES (?, ?, ?, 1, 100, 'add', 1)
                """,
                (guild_id, name, description),
            )
            perk_id = int(cur.lastrowid)
        return jsonify({"ok": True, "id": perk_id})

    @app.post("/api/perk/<int:perk_id>/update")
    def api_perk_update(perk_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "name" in data:
            name = str(data.get("name", "")).strip()
            if not name:
                return jsonify({"error": "name cannot be empty"}), 400
            updates["name"] = name
        if "description" in data:
            updates["description"] = str(data.get("description", ""))
        if "enabled" in data:
            try:
                updates["enabled"] = 1 if int(data.get("enabled")) != 0 else 0
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid enabled value"}), 400
        if "priority" in data:
            try:
                updates["priority"] = int(data.get("priority"))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid priority"}), 400
        if "stack_mode" in data:
            updates["stack_mode"] = _sanitize_stack_mode(data.get("stack_mode"))
        if "max_stacks" in data:
            try:
                updates["max_stacks"] = max(1, int(data.get("max_stacks")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid max_stacks"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400

        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.append(perk_id)
        with _connect() as conn:
            cur = conn.execute(
                f"UPDATE perks SET {set_clause} WHERE id = ?",
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Perk not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/perk/<int:perk_id>")
    def api_perk_delete(perk_id: int):
        with _connect() as conn:
            cur = conn.execute(
                "DELETE FROM perks WHERE id = ?",
                (perk_id,),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Perk not found"}), 404
        return jsonify({"ok": True})

    @app.post("/api/perk/<int:perk_id>/requirements")
    def api_perk_requirement_create(perk_id: int):
        data = request.get_json(silent=True) or {}
        try:
            group_id = max(1, int(data.get("group_id", 1)))
            value = max(0, int(float(data.get("value", 1))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid group/value"}), 400
        req_type = _normalize_req_type(data.get("req_type", "commodity_qty"))
        commodity_name = str(data.get("commodity_name", "")).strip()
        operator = _sanitize_operator(data.get("operator", ">="))
        if req_type not in {"commodity_qty", "tag_qty", "any_single_commodity_qty"}:
            return jsonify({"error": "Unsupported req_type"}), 400
        if req_type == "any_single_commodity_qty":
            commodity_name = "*"
        if not commodity_name:
            return jsonify({"error": "commodity_name/tag is required"}), 400
        with _connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM perks WHERE id = ?",
                (perk_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Perk not found"}), 404
            cur = conn.execute(
                """
                INSERT INTO perk_requirements (perk_id, group_id, req_type, commodity_name, operator, value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (perk_id, group_id, req_type, commodity_name, operator, value),
            )
        return jsonify({"ok": True, "id": int(cur.lastrowid)})

    @app.post("/api/perk/<int:perk_id>/requirements/<int:req_id>/update")
    def api_perk_requirement_update(perk_id: int, req_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "group_id" in data:
            try:
                updates["group_id"] = max(1, int(data.get("group_id")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid group_id"}), 400
        if "req_type" in data:
            req_type = _normalize_req_type(data.get("req_type", ""))
            if req_type not in {"commodity_qty", "tag_qty", "any_single_commodity_qty"}:
                return jsonify({"error": "Unsupported req_type"}), 400
            updates["req_type"] = req_type
            if req_type == "any_single_commodity_qty":
                updates["commodity_name"] = "*"
        if "commodity_name" in data:
            updates["commodity_name"] = str(data.get("commodity_name", "")).strip()
        if str(updates.get("req_type", "")).lower() == "any_single_commodity_qty":
            updates["commodity_name"] = "*"
        if "operator" in data:
            updates["operator"] = _sanitize_operator(data.get("operator"))
        if "value" in data:
            try:
                updates["value"] = max(0, int(float(data.get("value"))))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.extend([req_id, perk_id])
        with _connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE perk_requirements
                SET {set_clause}
                WHERE id = ? AND perk_id = ?
                """,
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Requirement not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/perk/<int:perk_id>/requirements/<int:req_id>")
    def api_perk_requirement_delete(perk_id: int, req_id: int):
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM perk_requirements
                WHERE id = ? AND perk_id = ?
                """,
                (req_id, perk_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Requirement not found"}), 404
        return jsonify({"ok": True})

    @app.post("/api/perk/<int:perk_id>/effects")
    def api_perk_effect_create(perk_id: int):
        data = request.get_json(silent=True) or {}
        target_stat = str(data.get("target_stat", "income")).strip().lower() or "income"
        value_mode = str(data.get("value_mode", "flat")).strip().lower() or "flat"
        scale_source = str(data.get("scale_source", "none")).strip().lower() or "none"
        scale_key = str(data.get("scale_key", "")).strip()
        try:
            value = float(data.get("value", 0))
            scale_factor = float(data.get("scale_factor", 0))
            cap = float(data.get("cap", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid numeric effect fields"}), 400

        with _connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM perks WHERE id = ?",
                (perk_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Perk not found"}), 404
            cur = conn.execute(
                """
                INSERT INTO perk_effects (
                    perk_id, effect_type, target_stat, value_mode, value,
                    scale_source, scale_key, scale_factor, cap
                )
                VALUES (?, 'stat_mod', ?, ?, ?, ?, ?, ?, ?)
                """,
                (perk_id, target_stat, value_mode, value, scale_source, scale_key, scale_factor, cap),
            )
        return jsonify({"ok": True, "id": int(cur.lastrowid)})

    @app.post("/api/perk/<int:perk_id>/effects/<int:effect_id>/update")
    def api_perk_effect_update(perk_id: int, effect_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        for text_key in {"target_stat", "value_mode", "scale_source", "scale_key"}:
            if text_key in data:
                updates[text_key] = str(data.get(text_key, "")).strip().lower()
        for num_key in {"value", "scale_factor", "cap"}:
            if num_key in data:
                try:
                    updates[num_key] = float(data.get(num_key))
                except (TypeError, ValueError):
                    return jsonify({"error": f"Invalid {num_key}"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.extend([effect_id, perk_id])
        with _connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE perk_effects
                SET {set_clause}
                WHERE id = ? AND perk_id = ?
                """,
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Effect not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/perk/<int:perk_id>/effects/<int:effect_id>")
    def api_perk_effect_delete(perk_id: int, effect_id: int):
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM perk_effects
                WHERE id = ? AND perk_id = ?
                """,
                (effect_id, perk_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Effect not found"}), 404
        return jsonify({"ok": True})

    @app.get("/api/bank-requests")
    def api_bank_requests():
        status = (request.args.get("status") or "pending").strip().lower()
        if status not in {"pending", "approved", "denied", "all"}:
            return jsonify({"error": "Invalid status"}), 400
        limit_raw = request.args.get("limit")
        try:
            limit = max(1, min(1000, int(limit_raw))) if limit_raw is not None else 300
        except ValueError:
            return jsonify({"error": "Invalid limit"}), 400

        where_status = "" if status == "all" else "WHERE br.status = ?"
        args: tuple[object, ...] = () if status == "all" else (status,)
        with _connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    br.id,
                    br.guild_id,
                    br.user_id,
                    COALESCE(u.display_name, '') AS display_name,
                    br.request_type,
                    br.amount,
                    br.reason,
                    br.status,
                    br.decision_reason,
                    br.reviewed_by,
                    br.created_at,
                    br.reviewed_at,
                    br.processed_at
                FROM bank_requests br
                LEFT JOIN users u
                  ON u.guild_id = br.guild_id
                 AND u.user_id = br.user_id
                {where_status}
                ORDER BY br.id DESC
                LIMIT ?
                """,
                (*args, limit),
            ).fetchall()
        return jsonify({"requests": [dict(r) for r in rows]})

    @app.post("/api/bank-requests/<int:request_id>/approve")
    def api_bank_request_approve(request_id: int):
        data = request.get_json(silent=True) or {}
        reason = str(data.get("reason", "")).strip()
        now = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            cur = conn.execute(
                """
                UPDATE bank_requests
                SET status = 'approved',
                    decision_reason = ?,
                    reviewed_by = 0,
                    reviewed_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (reason, now, request_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Request not found or already reviewed"}), 404
        return jsonify({"ok": True})

    @app.post("/api/bank-requests/<int:request_id>/deny")
    def api_bank_request_deny(request_id: int):
        data = request.get_json(silent=True) or {}
        reason = str(data.get("reason", "")).strip()
        now = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            cur = conn.execute(
                """
                UPDATE bank_requests
                SET status = 'denied',
                    decision_reason = ?,
                    reviewed_by = 0,
                    reviewed_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (reason, now, request_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Request not found or already reviewed"}), 404
        return jsonify({"ok": True})

    @app.post("/api/app-config/<config_name>/update")
    def api_app_config_update(config_name: str):
        if config_name not in APP_CONFIG_SPECS:
            return jsonify({"error": "Unknown app config"}), 404
        data = request.get_json(silent=True) or {}
        if "value" not in data:
            return jsonify({"error": "Missing value"}), 400
        try:
            value = _set_config(config_name, data.get("value"))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid value"}), 400
        return jsonify({"ok": True, "value": _config_value_for_json(config_name, value)})

    @app.post("/api/server-actions/backup")
    def api_server_action_backup():
        try:
            backup_path = _create_database_backup_now(prefix="manual")
        except Exception as exc:
            return jsonify({"error": f"Backup failed: {exc}"}), 500
        return jsonify({"ok": True, "backup_path": backup_path})

    @app.get("/api/server-actions/ticking")
    def api_server_action_ticking_get():
        raw = (_state_get("ticks_paused") or "").strip()
        paused = raw in {"1", "true", "True", "yes", "on"}
        return jsonify({"paused": paused})

    @app.post("/api/server-actions/ticking")
    def api_server_action_ticking_set():
        data = request.get_json(silent=True) or {}
        if "paused" not in data:
            return jsonify({"error": "paused is required"}), 400
        paused = bool(data.get("paused"))
        _state_set("ticks_paused", "1" if paused else "0")
        return jsonify({"ok": True, "paused": paused})

    @app.get("/api/server-actions/backups")
    def api_server_action_backups():
        backup_dir = _backup_dir()
        files = []
        for path in sorted(backup_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                stat = path.stat()
            except OSError:
                continue
            files.append(
                {
                    "name": path.name,
                    "size_bytes": int(stat.st_size),
                    "size_human": _human_size(int(stat.st_size)),
                    "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                }
            )
        return jsonify({"backups": files})

    @app.delete("/api/server-actions/backups/<backup_name>")
    def api_server_action_delete_backup(backup_name: str):
        if "/" in backup_name or "\\" in backup_name:
            return jsonify({"error": "Invalid backup name"}), 400
        if not backup_name.endswith(".db"):
            return jsonify({"error": "Only .db backup files are deletable"}), 400

        backup_dir = _backup_dir().resolve()
        target = (backup_dir / backup_name).resolve()
        if target.parent != backup_dir:
            return jsonify({"error": "Invalid backup path"}), 400
        if not target.exists():
            return jsonify({"error": "Backup not found"}), 404

        try:
            target.unlink()
        except OSError as exc:
            return jsonify({"error": f"Delete failed: {exc}"}), 500
        return jsonify({"ok": True, "deleted": backup_name})

    @app.get("/api/server-actions/backups/<backup_name>/download")
    def api_server_action_download_backup(backup_name: str):
        if "/" in backup_name or "\\" in backup_name:
            return jsonify({"error": "Invalid backup name"}), 400
        if not backup_name.endswith(".db"):
            return jsonify({"error": "Only .db backup files are downloadable"}), 400

        backup_dir = _backup_dir().resolve()
        target = (backup_dir / backup_name).resolve()
        if target.parent != backup_dir:
            return jsonify({"error": "Invalid backup path"}), 400
        if not target.exists():
            return jsonify({"error": "Backup not found"}), 404

        return send_file(
            target,
            as_attachment=True,
            download_name=backup_name,
            mimetype="application/x-sqlite3",
        )

    return app


def main() -> None:
    app = create_app()
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", "8082"))
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
