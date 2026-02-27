from __future__ import annotations

import os
import secrets
import sqlite3
import sys
import time
import socket
import json
import io
import hashlib
import shlex
import subprocess
import math
import random
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from flask import Flask, g, jsonify, redirect, render_template_string, request, send_file, session
from PIL import Image, UnidentifiedImageError

# Ensure project root is importable when running as a standalone script via systemd.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEMPLATE_DIR = Path(__file__).resolve().parent / "dashboard_templates"

def _load_template(name: str) -> str:
    return (TEMPLATE_DIR / name).read_text(encoding="utf-8")

from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME
from stockbot.config.runtime import APP_CONFIG_SPECS as CORE_APP_CONFIG_SPECS
from stockbot.db import (
    add_action_history,
    get_commodity,
    get_company,
    get_state_value,
    get_user,
    get_user_commodities,
    get_user_shares,
    recalc_user_networth,
    set_holding,
    set_state_value,
    update_company_trade_volume,
    update_user_networth,
    update_user_bank,
    upsert_user_commodity,
    upsert_holding,
)
from stockbot.services.activity import active_user_ids_last_day
from stockbot.services.jobs import get_active_jobs, refresh_jobs_rotation, swap_active_job
from stockbot.services.money import money
from stockbot.services.perks import evaluate_user_perks
from stockbot.services.shop_state import get_shop_items, refresh_shop, set_item_availability, sold_key, swap_item

@dataclass(frozen=True)
class _CfgSpec:
    default: object
    cast: type
    description: str


APP_CONFIG_SPECS: dict[str, _CfgSpec] = {
    name: _CfgSpec(
        spec.default,
        spec.cast if spec.cast in {int, float, str} else str,
        spec.description,
    )
    for name, spec in CORE_APP_CONFIG_SPECS.items()
}
APP_CONFIG_SPECS.update({
    "IMAGE_UPLOAD_MAX_SOURCE_MB": _CfgSpec(8, int, "Max download size (MB) allowed when ingesting image URLs."),
    "IMAGE_UPLOAD_MAX_DIM": _CfgSpec(1024, int, "Max width/height in pixels for ingested images."),
    "IMAGE_UPLOAD_MAX_STORED_KB": _CfgSpec(300, int, "Max stored file size in KB per ingested image."),
    "IMAGE_UPLOAD_TOTAL_GB": _CfgSpec(2.0, float, "Total local uploads quota in GB for dashboard-hosted images."),
})

AUTH_REQUIRED_HTML = _load_template("auth_required.html")


MAIN_HTML = _load_template("main.html")


DETAIL_HTML = _load_template("company_detail.html")

COMMODITY_DETAIL_HTML = _load_template("commodity_detail.html")

PLAYER_DETAIL_HTML = _load_template("player_detail.html")

APP_CONFIG_DETAIL_HTML = _load_template("app_config_detail.html")

PERK_DETAIL_HTML = _load_template("perk_detail.html")

JOB_DETAIL_HTML = _load_template("job_detail.html")
PROPERTY_DETAIL_HTML = _load_template("property_detail.html")


def create_app() -> Flask:
    app = Flask(__name__)
    DISABLED_PERK_EFFECT_TARGETS = {"slot_bet_multiplier", "steal_chance", "steal_amount_min", "steal_amount_max"}
    repo_root = Path(__file__).resolve().parents[1]
    db_path = Path(os.getenv("DASHBOARD_DB_PATH", str(repo_root / "data" / "stockbot.db")))
    auth_cookie = "webadmin_session"

    def _read_env_deploy_value(key: str) -> str:
        env_file = repo_root / ".env.deploy"
        if not env_file.exists():
            return ""
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                text = line.strip()
                if not text or text.startswith("#") or "=" not in text:
                    continue
                k, v = text.split("=", 1)
                if k.strip() != key:
                    continue
                value = v.strip()
                if len(value) >= 2 and (
                    (value.startswith('"') and value.endswith('"'))
                    or (value.startswith("'") and value.endswith("'"))
                ):
                    value = value[1:-1]
                return value.strip()
        except Exception:
            return ""
        return ""

    # Single source of truth for dashboard godtoken: .env.deploy
    godtoken_env = (
        _read_env_deploy_value("WEBADMIN_GODTOKEN")
        or _read_env_deploy_value("GODTOKEN")
    )
    session_secret = (
        _read_env_deploy_value("WEBADMIN_SESSION_SECRET")
        or os.getenv("WEBADMIN_SESSION_SECRET", "").strip()
        or os.getenv("FLASK_SECRET_KEY", "").strip()
    )
    if not session_secret:
        session_secret = secrets.token_hex(32)
    app.secret_key = session_secret
    app.config["SESSION_COOKIE_NAME"] = auth_cookie
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 10000;")
        conn.execute("PRAGMA foreign_keys = ON;")
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

    def _cleanup_expired_auth_entries() -> None:
        now_epoch = int(time.time())
        with _connect() as conn:
            conn.execute(
                """
                DELETE FROM app_state
                WHERE key LIKE 'webadmin:token:%'
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
            return True

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
            "SLOT_BET_MIN",
            "SLOT_BET_MAX",
            "SLOT_PAIR_PAYOUT_BASE",
            "SLOT_HOURLY_SPIN_LIMIT",
            "SLOT_TRIPLE_MULT_CHERRY",
            "SLOT_TRIPLE_MULT_LEMON",
            "SLOT_TRIPLE_MULT_WATERMELON",
            "SLOT_TRIPLE_MULT_BELL",
            "SLOT_TRIPLE_MULT_STAR",
            "SLOT_TRIPLE_MULT_SEVEN",
            "SLOT_TRIPLE_MULT_DIAMOND",
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
            if name in {"SLOT_BET_MIN", "SLOT_BET_MAX"}:
                return max(0.01, number)
            if name == "SLOT_PAIR_PAYOUT_BASE":
                return max(0.0, number)
            if name in {
                "SLOT_TRIPLE_MULT_CHERRY",
                "SLOT_TRIPLE_MULT_LEMON",
                "SLOT_TRIPLE_MULT_WATERMELON",
                "SLOT_TRIPLE_MULT_BELL",
                "SLOT_TRIPLE_MULT_STAR",
                "SLOT_TRIPLE_MULT_SEVEN",
                "SLOT_TRIPLE_MULT_DIAMOND",
            }:
                return max(0.0, number)
            if name == "SLOT_HOURLY_SPIN_LIMIT":
                return max(0.0, number)
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
            "ACTIVITY_APPLICATION_ID",
            "COMMODITIES_LIMIT",
            "IMAGE_UPLOAD_MAX_SOURCE_MB",
            "IMAGE_UPLOAD_MAX_DIM",
            "IMAGE_UPLOAD_MAX_STORED_KB",
            "SLOT_MAX_SPINS",
        }:
            if name in {"ANNOUNCEMENT_CHANNEL_ID", "GM_ID", "ACTIVITY_APPLICATION_ID"}:
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
        forwarded_proto = str(request.headers.get("X-Forwarded-Proto", "")).split(",", 1)[0].strip().lower()
        if forwarded_proto in {"http", "https"}:
            return f"{forwarded_proto}://{request.host}"
        return request.host_url.rstrip("/")

    def _db_access_url() -> str:
        configured = str(os.getenv("DASHBOARD_DB_ACCESS_URL", "")).strip()
        if configured:
            return configured
        host = request.host.split(":", 1)[0]
        return f"http://{host}:8081"

    def _is_port_open(host: str, port: int, timeout_seconds: float = 0.4) -> bool:
        try:
            with socket.create_connection((host, int(port)), timeout=timeout_seconds):
                return True
        except OSError:
            return False

    def _maybe_start_sqlweb_on_demand(target_url: str) -> None:
        enabled_raw = str(os.getenv("SQLWEB_ONDEMAND_ENABLED", "1")).strip().lower()
        if enabled_raw not in {"1", "true", "yes", "on"}:
            return

        parsed = urlparse(target_url)
        host = parsed.hostname or "127.0.0.1"
        port = int(parsed.port or 8081)
        if _is_port_open(host, port):
            return

        explicit_cmd = str(os.getenv("SQLWEB_ONDEMAND_CMD", "")).strip()
        if explicit_cmd:
            cmd = shlex.split(explicit_cmd)
        else:
            max_seconds = int(os.getenv("SQLWEB_MAX_SECONDS", "1800"))
            cmd = [
                sys.executable,
                str(repo_root / "scripts" / "admin_sqlite_web.py"),
                "--host",
                host,
                "--port",
                str(port),
                "--max-seconds",
                str(max(0, max_seconds)),
            ]
        try:
            subprocess.Popen(
                cmd,
                cwd=str(repo_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception:
            return

        deadline = time.time() + 8.0
        while time.time() < deadline:
            if _is_port_open(host, port):
                return
            time.sleep(0.2)

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

    def _canonicalize_media_url(raw_value: object) -> str:
        text = str(raw_value or "").strip()
        if not text:
            return ""
        filename = _filename_from_stored_image_url(text)
        if not filename:
            return text
        return _media_url_for(filename)

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
            SELECT image_url FROM properties WHERE image_url != ''
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

    def _ensure_properties_tables() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS properties (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    image_url TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    max_level INTEGER NOT NULL DEFAULT 5,
                    buy_price REAL NOT NULL DEFAULT 0,
                    level_costs_json TEXT NOT NULL DEFAULT '[0,0,0,0,0]',
                    level_effects_json TEXT NOT NULL DEFAULT '[[],[],[],[],[]]',
                    ascension_cost REAL NOT NULL DEFAULT 0,
                    ascension_effects_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL,
                    UNIQUE(guild_id, name)
                );

                CREATE TABLE IF NOT EXISTS user_properties (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    property_id INTEGER NOT NULL,
                    level INTEGER NOT NULL DEFAULT 0,
                    ascended INTEGER NOT NULL DEFAULT 0,
                    ascended_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, user_id, property_id),
                    FOREIGN KEY (guild_id, user_id)
                        REFERENCES users (guild_id, user_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (property_id)
                        REFERENCES properties (id)
                        ON DELETE CASCADE
                );
                """
            )
            prop_cols = {row["name"] for row in conn.execute("PRAGMA table_info(properties);").fetchall()}
            if prop_cols and "level_costs_json" not in prop_cols:
                conn.execute("ALTER TABLE properties ADD COLUMN level_costs_json TEXT NOT NULL DEFAULT '[0,0,0,0,0]';")
            if prop_cols and "level_effects_json" not in prop_cols:
                conn.execute("ALTER TABLE properties ADD COLUMN level_effects_json TEXT NOT NULL DEFAULT '[[],[],[],[],[]]';")
            if prop_cols and "ascension_cost" not in prop_cols:
                conn.execute("ALTER TABLE properties ADD COLUMN ascension_cost REAL NOT NULL DEFAULT 0;")
            if prop_cols and "ascension_effects_json" not in prop_cols:
                conn.execute("ALTER TABLE properties ADD COLUMN ascension_effects_json TEXT NOT NULL DEFAULT '[]';")
            if prop_cols and "max_level" not in prop_cols:
                conn.execute("ALTER TABLE properties ADD COLUMN max_level INTEGER NOT NULL DEFAULT 5;")
            up_cols = {row["name"] for row in conn.execute("PRAGMA table_info(user_properties);").fetchall()}
            if up_cols and "ascended" not in up_cols:
                conn.execute("ALTER TABLE user_properties ADD COLUMN ascended INTEGER NOT NULL DEFAULT 0;")
            if up_cols and "ascended_count" not in up_cols:
                conn.execute("ALTER TABLE user_properties ADD COLUMN ascended_count INTEGER NOT NULL DEFAULT 0;")

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

    def _has_disabled_perk_effect_target(raw: object) -> bool:
        effects: list[dict] = []
        if isinstance(raw, dict):
            effects = [raw]
        elif isinstance(raw, list):
            effects = [e for e in raw if isinstance(e, dict)]
        else:
            return False
        for eff in effects:
            target = str(eff.get("target_stat", "") or "").strip().lower()
            if target in DISABLED_PERK_EFFECT_TARGETS:
                return True
        return False

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

    def _parse_effects_json(raw: object) -> list[dict]:
        if isinstance(raw, dict):
            return [raw]
        if isinstance(raw, list):
            return [e for e in raw if isinstance(e, dict)]
        text = str(raw or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        return _parse_effects_json(parsed)

    def _normalize_req_type(raw: object) -> str:
        req_type = str(raw or "commodity_qty").strip().lower() or "commodity_qty"
        if req_type == "any_single_commodities_qty":
            return "any_single_commodity_qty"
        return req_type

    def _activity_perk_base_income(guild_id: int, user_id: int) -> float:
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT rank
                FROM users
                WHERE guild_id = ? AND user_id = ?
                LIMIT 1
                """,
                (guild_id, user_id),
            ).fetchone()
        rank = str(row["rank"] or DEFAULT_RANK) if row is not None else DEFAULT_RANK
        lookup = {k.lower(): float(v) for k, v in RANK_INCOME.items()}
        return lookup.get(rank.lower(), float(RANK_INCOME.get(DEFAULT_RANK, 0.0)))

    def _matched_perk_map_for_user(guild_id: int, user_id: int) -> dict[int, str]:
        base_income = _activity_perk_base_income(guild_id, user_id)
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
        out: dict[int, str] = {}
        for row in list(result.get("matched_perks", [])):
            try:
                perk_id = int(row.get("perk_id", 0))
            except (TypeError, ValueError):
                continue
            if perk_id <= 0:
                continue
            out[perk_id] = str(row.get("name", f"perk#{perk_id}"))
        return out

    def _active_perk_ids_from_state(guild_id: int, user_id: int) -> set[int]:
        prefix = f"perk_active:{guild_id}:{user_id}:"
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT key, value
                FROM app_state
                WHERE key LIKE ?
                """,
                (f"{prefix}%",),
            ).fetchall()
        active_ids: set[int] = set()
        for row in rows:
            value = str(row["value"] or "").strip()
            if value != "1":
                continue
            key = str(row["key"] or "")
            try:
                active_ids.add(int(key.rsplit(":", 1)[-1]))
            except (TypeError, ValueError):
                continue
        return active_ids

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
        if name in {"ANNOUNCEMENT_CHANNEL_ID", "GM_ID", "ACTIVITY_APPLICATION_ID"}:
            return str(value)
        return value

    def _slot_config_payload() -> dict[str, object]:
        keys = (
            "SLOT_BET_MIN",
            "SLOT_BET_MAX",
            "SLOT_MAX_SPINS",
            "SLOT_HOURLY_SPIN_LIMIT",
            "SLOT_PAIR_PAYOUT_BASE",
            "SLOT_TRIPLE_MULT_CHERRY",
            "SLOT_TRIPLE_MULT_LEMON",
            "SLOT_TRIPLE_MULT_WATERMELON",
            "SLOT_TRIPLE_MULT_BELL",
            "SLOT_TRIPLE_MULT_STAR",
            "SLOT_TRIPLE_MULT_SEVEN",
            "SLOT_TRIPLE_MULT_DIAMOND",
        )
        return {k: _get_config(k) for k in keys}

    def _dual_game_prefix(guild_id: int) -> str:
        return f"dual:game:{guild_id}:"

    def _dual_game_key(guild_id: int, code: str) -> str:
        return f"{_dual_game_prefix(guild_id)}{code.upper()}"

    def _dual_generate_code(guild_id: int) -> str:
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        for _ in range(100):
            code = "".join(random.choice(alphabet) for _ in range(6))
            if _state_get(_dual_game_key(guild_id, code)) is None:
                return code
        raise RuntimeError("Could not generate a unique DUAL code.")

    def _dual_load_game(guild_id: int, code: str) -> dict | None:
        raw = _state_get(_dual_game_key(guild_id, code))
        if not raw:
            return None
        try:
            game = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return game if isinstance(game, dict) else None

    def _dual_save_game(guild_id: int, game: dict) -> None:
        code = str(game.get("code", "")).strip().upper()
        if not code:
            raise ValueError("DUAL game code missing.")
        game["updated_at"] = datetime.now(timezone.utc).isoformat()
        _state_set(_dual_game_key(guild_id, code), json.dumps(game, separators=(",", ":")))

    def _dual_all_games(guild_id: int) -> list[dict]:
        prefix = _dual_game_prefix(guild_id)
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT key, value
                FROM app_state
                WHERE key LIKE ?
                """,
                (f"{prefix}%",),
            ).fetchall()
        out: list[dict] = []
        for row in rows:
            try:
                parsed = json.loads(str(row["value"] or ""))
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            code = str(parsed.get("code", "")).strip().upper()
            if code:
                parsed["code"] = code
                out.append(parsed)
        return out

    def _dual_find_game_for_user(guild_id: int, user_id: int) -> dict | None:
        user_key = str(int(user_id))
        for game in _dual_all_games(guild_id):
            status = str(game.get("status", "lobby"))
            if status not in {"lobby", "active"}:
                continue
            players = game.get("players")
            if isinstance(players, dict) and user_key in players:
                return game
        return None

    def _dual_game_public_view(game: dict) -> dict[str, object]:
        players_raw = game.get("players")
        players: list[dict[str, object]] = []
        ready_count = 0
        if isinstance(players_raw, dict):
            for uid, row in players_raw.items():
                if not isinstance(row, dict):
                    continue
                is_ready = bool(row.get("ready", False))
                if is_ready:
                    ready_count += 1
                players.append(
                    {
                        "user_id": str(uid),
                        "display_name": str(row.get("display_name", f"User {uid}")),
                        "bet": float(row.get("bet", 0.0) or 0.0),
                        "ready": is_ready,
                        "ready_bet": float(row.get("ready_bet", 0.0) or 0.0),
                        "last_guess": row.get("last_guess"),
                        "last_hint": str(row.get("last_hint", "")),
                        "guessed_round": int(row.get("guessed_round", 0) or 0),
                    }
                )
        players.sort(key=lambda p: str(p.get("display_name", "")).lower())
        return {
            "code": str(game.get("code", "")).upper(),
            "host_user_id": str(game.get("host_user_id", "")),
            "status": str(game.get("status", "lobby")),
            "pot": float(game.get("pot", 0.0) or 0.0),
            "round": int(game.get("round", 1) or 1),
            "min_value": int(game.get("min_value", 0) or 0),
            "max_value": int(game.get("max_value", 100) or 100),
            "player_count": len(players),
            "ready_count": ready_count,
            "players": players,
            "winner_user_id": str(game.get("winner_user_id", "")) if game.get("winner_user_id") else "",
            "created_at": str(game.get("created_at", "")),
            "updated_at": str(game.get("updated_at", "")),
        }

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

    def _until_next_tick_seconds() -> int:
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        last_tick_epoch_raw = _state_get("last_tick_epoch")
        try:
            last_tick_epoch = float(last_tick_epoch_raw) if last_tick_epoch_raw is not None else 0.0
        except ValueError:
            last_tick_epoch = 0.0
        if last_tick_epoch <= 0.0:
            return tick_interval
        remaining = int(math.ceil((last_tick_epoch + tick_interval) - time.time()))
        if remaining < 0:
            return 0
        return remaining

    _ensure_config_defaults()
    _ensure_perk_tables()
    _ensure_jobs_tables()
    _ensure_properties_tables()
    _ensure_commodity_tags_table()
    _ensure_commodities_columns()
    _ensure_company_owners_table()

    @app.before_request
    def _auth_gate():
        _cleanup_expired_auth_entries()
        g.webadmin_authenticated = False
        g.webadmin_read_only = True
        public_paths = {
            "/",
            "/api/stocks",
            "/api/dashboard-stats",
            "/api/app-config/TICK_INTERVAL",
        }
        public_post_paths = {
            "/api/activity/oauth/token",
        }
        is_activity_auth_post = request.method == "POST" and request.path.startswith("/api/activity/")
        is_api_or_media = request.path.startswith("/api/") or request.path.startswith("/media/uploads/")
        if request.method == "OPTIONS" and (
            request.path in public_paths
            or request.path in public_post_paths
            or request.path.startswith("/api/activity/")
            or is_api_or_media
        ):
            return ("", 204)
        if request.path == "/favicon.ico":
            return ("", 204)

        token = (request.args.get("token") or "").strip()
        godtoken_ok = bool(token) and bool(godtoken_env) and token == godtoken_env
        session_auth = bool(session.get("webadmin_authenticated"))
        if session_auth:
            g.webadmin_authenticated = True
            g.webadmin_read_only = False
            return None
        if token:
            if godtoken_ok or _consume_one_time_token(token):
                session["webadmin_authenticated"] = True
                g.webadmin_authenticated = True
                g.webadmin_read_only = False
                return None

        # Public read-only mode for dashboard + market data endpoints.
        if request.method == "GET" and (request.path in public_paths or is_api_or_media):
            return None
        if is_activity_auth_post or (request.method == "POST" and request.path in public_post_paths):
            return None
        return render_template_string(AUTH_REQUIRED_HTML), 401

    @app.after_request
    def _set_session_cookie(resp):
        # CORS for Activity/UI cross-origin reads.
        if request.path.startswith("/api/") or request.path.startswith("/media/uploads/"):
            origin = (request.headers.get("Origin") or "").strip()
            # Some reverse-proxy setups may drop Origin before Flask sees it.
            # Always emit CORS for API/media reads so Activity can consume data.
            resp.headers["Access-Control-Allow-Origin"] = origin or "*"
            resp.headers["Vary"] = "Origin"
            resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
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
        db_access_url = _db_access_url()
        token = (request.args.get("token") or "").strip()
        db_access_entry_url = "/open-db-access"
        if token:
            db_access_entry_url = f"{db_access_entry_url}?token={quote(token)}"
        return render_template_string(
            MAIN_HTML,
            db_access_url=db_access_url,
            db_access_entry_url=db_access_entry_url,
            read_only_mode=bool(getattr(g, "webadmin_read_only", True)),
        )

    @app.get("/open-db-access")
    def open_db_access():
        db_access_url = _db_access_url()
        token = (request.args.get("token") or "").strip()
        if token:
            sep = "&" if "?" in db_access_url else "?"
            db_access_url = f"{db_access_url}{sep}token={quote(token)}"
        _maybe_start_sqlweb_on_demand(db_access_url)
        return redirect(db_access_url, code=302)

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

    @app.get("/property/<int:property_id>")
    def property_page(property_id: int):
        return render_template_string(PROPERTY_DETAIL_HTML, property_id=property_id)

    @app.get("/api/stocks")
    def api_stocks():
        tz_name = str(_get_config("DISPLAY_TIMEZONE"))
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
        local_today = datetime.now(tz).date().isoformat()

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
            prev_close_rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        guild_id,
                        symbol,
                        close_price,
                        date,
                        ROW_NUMBER() OVER (PARTITION BY guild_id, symbol ORDER BY date DESC) AS rn
                    FROM daily_close
                    WHERE date < ?
                )
                SELECT guild_id, symbol, close_price, date
                FROM ranked
                WHERE rn = 1
                """,
                (local_today,),
            ).fetchall()
            latest_close_rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        guild_id,
                        symbol,
                        close_price,
                        date,
                        ROW_NUMBER() OVER (PARTITION BY guild_id, symbol ORDER BY date DESC) AS rn
                    FROM daily_close
                )
                SELECT guild_id, symbol, close_price, date
                FROM ranked
                WHERE rn = 1
                """
            ).fetchall()
            user_id_raw = str(request.args.get("user_id", "") or "").strip()
            user_holdings_rows = []
            if user_id_raw.startswith("+"):
                user_id_raw = user_id_raw[1:]
            if user_id_raw.isdigit():
                user_holdings_rows = conn.execute(
                    """
                    SELECT symbol, shares
                    FROM holdings
                    WHERE guild_id = ? AND user_id = ? AND shares > 0
                    """,
                    (int(rows[0]["guild_id"]) if rows else 0, int(user_id_raw)),
                ).fetchall()

        history_map: dict[str, list[float]] = {}
        for h in history_rows:
            sym = str(h["symbol"])
            history_map.setdefault(sym, []).append(float(h["price"]))
        owners_map: dict[tuple[int, str], list[int]] = {}
        for o in owner_rows:
            key = (int(o["guild_id"]), str(o["symbol"]))
            owners_map.setdefault(key, []).append(int(o["user_id"]))
        prev_close_map: dict[tuple[int, str], tuple[float, str]] = {}
        for c in prev_close_rows:
            key = (int(c["guild_id"]), str(c["symbol"]))
            prev_close_map[key] = (float(c["close_price"]), str(c["date"]))
        latest_close_map: dict[tuple[int, str], tuple[float, str]] = {}
        for c in latest_close_rows:
            key = (int(c["guild_id"]), str(c["symbol"]))
            latest_close_map[key] = (float(c["close_price"]), str(c["date"]))
        user_holdings_map: dict[str, int] = {}
        for h in user_holdings_rows:
            user_holdings_map[str(h["symbol"])] = int(h["shares"])
        stocks: list[dict] = []
        for r in rows:
            row = dict(r)
            key = (int(r["guild_id"]), str(r["symbol"]))
            owner_ids = owners_map.get((int(r["guild_id"]), str(r["symbol"])), [])
            if not owner_ids:
                legacy_owner = int(row.get("owner_user_id", 0) or 0)
                owner_ids = [legacy_owner] if legacy_owner > 0 else []
            row["owner_user_ids"] = [str(uid) for uid in owner_ids]
            row["owner_user_id"] = str(owner_ids[0]) if owner_ids else "0"
            row["history_prices"] = history_map.get(str(r["symbol"]), [])
            prev_close = prev_close_map.get(key) or latest_close_map.get(key)
            if prev_close is not None:
                row["previous_close_price"] = float(prev_close[0])
                row["previous_close_date"] = str(prev_close[1])
            else:
                row["previous_close_price"] = None
                row["previous_close_date"] = ""
            row["owned_shares"] = int(user_holdings_map.get(str(r["symbol"]), 0))
            stocks.append(row)
        return jsonify(
            {
                "server_time_utc": datetime.now(timezone.utc).isoformat(),
                "stocks": stocks,
            }
        )

    def _normalize_owner_ids(raw: object) -> list[int]:
        if not isinstance(raw, list):
            return []
        out: list[int] = []
        seen: set[int] = set()
        for item in raw:
            try:
                uid = int(item)
            except (TypeError, ValueError):
                continue
            if uid <= 0 or uid in seen:
                continue
            seen.add(uid)
            out.append(uid)
        return out

    def _limit_bucket_key(guild_id: int, user_id: int, bucket_index: int) -> str:
        return f"trade_used:{guild_id}:{user_id}:{bucket_index}"

    def _cost_basis_key(guild_id: int, user_id: int, symbol: str) -> str:
        return f"cost_basis:{guild_id}:{user_id}:{symbol}"

    def _get_cost_basis(guild_id: int, user_id: int, symbol: str) -> float:
        raw = _state_get(_cost_basis_key(guild_id, user_id, symbol))
        if raw is None:
            return 0.0
        try:
            return max(0.0, float(raw))
        except ValueError:
            return 0.0

    def _set_cost_basis(guild_id: int, user_id: int, symbol: str, value: float) -> None:
        _state_set(_cost_basis_key(guild_id, user_id, symbol), f"{max(0.0, float(value)):.6f}")

    def _get_current_tick() -> int:
        raw = _state_get("last_tick")
        if raw is None:
            return 0
        try:
            return max(0, int(raw))
        except ValueError:
            return 0

    def _enforce_and_consume_limit(guild_id: int, user_id: int, shares: int) -> tuple[bool, str]:
        limit = int(_get_config("TRADING_LIMITS"))
        period = int(_get_config("TRADING_LIMITS_PERIOD"))
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        if limit <= 0:
            return True, ""
        if period <= 0:
            return False, "Trading limits misconfigured: TRADING_LIMITS_PERIOD must be > 0."

        evaluated = evaluate_user_perks(
            guild_id=guild_id,
            user_id=user_id,
            base_income=0.0,
            base_trade_limits=limit,
            base_networth=0.0,
        )
        limit = int(evaluated["final"]["trade_limits"])
        if limit <= 0:
            period_minutes = (period * tick_interval) / 60.0
            return (
                False,
                (
                    f"Trade limit reached for this period. Remaining: 0 shares "
                    f"(limit {limit} per {period_minutes:.2f} minutes)."
                ),
            )

        tick = _get_current_tick()
        bucket = tick // period
        key = _limit_bucket_key(guild_id, user_id, bucket)
        used_raw = _state_get(key)
        used = int(float(used_raw)) if used_raw is not None else 0
        if used + shares > limit:
            remaining = max(0, limit - used)
            period_minutes = (period * tick_interval) / 60.0
            return (
                False,
                (
                    f"Trade limit reached for this period. Remaining: {remaining} shares "
                    f"(limit {limit} per {period_minutes:.2f} minutes)."
                ),
            )

        _state_set(key, str(used + shares))
        return True, ""

    @app.post("/api/activity/trade/buy")
    def api_activity_trade_buy():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        symbol = str(data.get("symbol", "") or "").strip().upper()
        try:
            shares = int(float(data.get("shares", 0)))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid shares"}), 400
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if shares <= 0:
            return jsonify({"error": "Shares must be a positive integer."}), 400
        if not symbol:
            return jsonify({"error": "symbol is required"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        if guild_id <= 0:
            return jsonify({"error": "No guild data"}), 404

        user_id = int(user_id_raw)
        user = get_user(guild_id, user_id)
        if user is None:
            return jsonify({"error": "User not registered"}), 404
        company = get_company(guild_id, symbol)
        if company is None:
            return jsonify({"error": f"Company `{symbol}` not found."}), 404

        owner_user_ids = _normalize_owner_ids(company.get("owner_user_ids", []))
        if user_id in owner_user_ids:
            return jsonify({"error": "Company owners cannot buy their own stock."}), 400

        price = float(company.get("current_price", company["base_price"]))
        total_cost = round(price * shares, 2)
        if float(user.get("bank", 0.0)) < total_cost:
            return jsonify({"error": f"Not enough funds. Need ${total_cost:.2f}."}), 400

        ok, limit_msg = _enforce_and_consume_limit(guild_id, user_id, shares)
        if not ok:
            return jsonify({"error": limit_msg}), 400

        owned_before = get_user_shares(guild_id, user_id, symbol)
        cost_basis_before = _get_cost_basis(guild_id, user_id, symbol)
        if cost_basis_before <= 0 and owned_before > 0:
            cost_basis_before = float(owned_before) * price

        new_bank = float(user.get("bank", 0.0)) - total_cost
        update_user_bank(guild_id, user_id, new_bank)
        upsert_holding(guild_id, user_id, symbol, shares)
        _set_cost_basis(guild_id, user_id, symbol, cost_basis_before + total_cost)
        update_company_trade_volume(
            guild_id,
            symbol,
            float(shares),
            0.0,
            datetime.now(timezone.utc).isoformat(),
        )
        add_action_history(
            guild_id=guild_id,
            user_id=user_id,
            action_type="buy",
            target_type="stock",
            target_symbol=symbol,
            quantity=float(shares),
            unit_price=price,
            total_amount=total_cost,
            details="source=activity",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        active_key = f"user_last_active:{guild_id}:{user_id}"
        now_ts = str(time.time())
        _state_set(active_key, now_ts)
        set_state_value(active_key, now_ts)

        owner_fee_rate_pct = max(0.0, min(100.0, float(_get_config("OWNER_BUY_FEE_RATE"))))
        owner_fee = 0.0
        if owner_user_ids and owner_fee_rate_pct > 0:
            total_owner_fee = round(total_cost * (owner_fee_rate_pct / 100.0), 2)
            if total_owner_fee > 0:
                eligible_owner_ids = [oid for oid in owner_user_ids if get_user(guild_id, oid) is not None]
                if eligible_owner_ids:
                    cents = int(round(total_owner_fee * 100))
                    count = len(eligible_owner_ids)
                    base_cents = cents // count
                    rem = cents % count
                    splits = [base_cents + (1 if i < rem else 0) for i in range(count)]
                    for idx, owner_id in enumerate(eligible_owner_ids):
                        part = splits[idx] / 100.0
                        if part <= 0:
                            continue
                        owner_user = get_user(guild_id, owner_id)
                        if owner_user is None:
                            continue
                        owner_fee += part
                        update_user_bank(guild_id, owner_id, float(owner_user.get("bank", 0.0)) + part)
                        add_action_history(
                            guild_id=guild_id,
                            user_id=owner_id,
                            action_type="owner_payout",
                            target_type="stock",
                            target_symbol=symbol,
                            quantity=float(shares),
                            unit_price=part / max(1, shares),
                            total_amount=part,
                            details=f"source=buy;buyer={user_id};rate={owner_fee_rate_pct:.2f}%;owners={count};via=activity",
                            created_at=datetime.now(timezone.utc).isoformat(),
                        )
                    owner_fee = round(owner_fee, 2)

        new_owned = get_user_shares(guild_id, user_id, symbol)
        return jsonify(
            {
                "ok": True,
                "symbol": symbol,
                "shares_delta": shares,
                "owned_shares": int(new_owned),
                "unit_price": price,
                "total_cost": total_cost,
                "owner_fee": owner_fee,
                "bank": float(new_bank),
            }
        )

    @app.post("/api/activity/trade/sell")
    def api_activity_trade_sell():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        symbol = str(data.get("symbol", "") or "").strip().upper()
        try:
            shares = int(float(data.get("shares", 0)))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid shares"}), 400
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if shares <= 0:
            return jsonify({"error": "Shares must be a positive integer."}), 400
        if not symbol:
            return jsonify({"error": "symbol is required"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        if guild_id <= 0:
            return jsonify({"error": "No guild data"}), 404

        user_id = int(user_id_raw)
        user = get_user(guild_id, user_id)
        if user is None:
            return jsonify({"error": "User not registered"}), 404
        company = get_company(guild_id, symbol)
        if company is None:
            return jsonify({"error": f"Company `{symbol}` not found."}), 404

        owner_user_ids = _normalize_owner_ids(company.get("owner_user_ids", []))
        if user_id in owner_user_ids:
            return jsonify({"error": "Company owners cannot sell their own stock."}), 400

        owned = get_user_shares(guild_id, user_id, symbol)
        if owned < shares:
            return jsonify({"error": f"You only own {owned} shares of {symbol}."}), 400

        ok, limit_msg = _enforce_and_consume_limit(guild_id, user_id, shares)
        if not ok:
            return jsonify({"error": limit_msg}), 400

        price = float(company.get("current_price", company["base_price"]))
        gross_gain = float(price * shares)
        cost_basis_before = _get_cost_basis(guild_id, user_id, symbol)
        if cost_basis_before <= 0 and owned > 0:
            cost_basis_before = float(owned) * price
        avg_cost = (cost_basis_before / float(owned)) if owned > 0 else price
        cost_removed = avg_cost * shares
        realized_profit = max(0.0, gross_gain - cost_removed)
        fee_ratio = max(0.0, float(_get_config("TRADING_FEES"))) / 100.0
        fee = realized_profit * fee_ratio
        net_gain = round(max(0.0, gross_gain - fee), 2)

        new_bank = float(user.get("bank", 0.0)) + net_gain
        update_user_bank(guild_id, user_id, new_bank)
        remaining = owned - shares
        set_holding(guild_id, user_id, symbol, remaining)
        new_cost_basis = max(0.0, cost_basis_before - cost_removed)
        if remaining <= 0:
            new_cost_basis = 0.0
        _set_cost_basis(guild_id, user_id, symbol, new_cost_basis)
        update_company_trade_volume(
            guild_id,
            symbol,
            0.0,
            float(shares),
            datetime.now(timezone.utc).isoformat(),
        )
        add_action_history(
            guild_id=guild_id,
            user_id=user_id,
            action_type="sell",
            target_type="stock",
            target_symbol=symbol,
            quantity=float(shares),
            unit_price=price,
            total_amount=net_gain,
            details=f"gross={gross_gain:.2f};fee={fee:.2f};source=activity",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        active_key = f"user_last_active:{guild_id}:{user_id}"
        now_ts = str(time.time())
        _state_set(active_key, now_ts)
        set_state_value(active_key, now_ts)

        return jsonify(
            {
                "ok": True,
                "symbol": symbol,
                "shares_delta": -shares,
                "owned_shares": int(max(0, remaining)),
                "unit_price": price,
                "gross_gain": round(gross_gain, 2),
                "fee": round(fee, 2),
                "net_gain": net_gain,
                "bank": float(new_bank),
                "avg_buy_price": round(avg_cost, 4),
                "estimated_pl_before_fee": round(gross_gain - cost_removed, 2),
                "estimated_pl_after_fee": round(net_gain - cost_removed, 2),
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
                "until_tick": f"{_until_next_tick_seconds() / 60.0:.2f} min",
                "seconds_until_tick": _until_next_tick_seconds(),
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

    @app.route("/api/activity/oauth/token", methods=["GET", "POST"])
    def api_activity_oauth_token():
        if request.method == "GET":
            code = str(request.args.get("code", "") or "").strip()
            redirect_uri = str(request.args.get("redirect_uri", "") or "").strip()
        else:
            data = request.get_json(silent=True) or {}
            code = str(data.get("code", "") or "").strip()
            redirect_uri = str(data.get("redirect_uri", "") or "").strip()
        if not code:
            return jsonify({"error": "Missing code"}), 400

        client_id = int(_get_config("ACTIVITY_APPLICATION_ID") or 0)
        if client_id <= 0:
            return jsonify({"error": "ACTIVITY_APPLICATION_ID is not configured"}), 400

        client_secret = str(os.getenv("ACTIVITY_DISCORD_CLIENT_SECRET", "") or "").strip()
        if not client_secret:
            return jsonify({"error": "ACTIVITY_DISCORD_CLIENT_SECRET is not configured"}), 500

        form = {
            "client_id": str(client_id),
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "code": code,
        }
        if redirect_uri:
            form["redirect_uri"] = redirect_uri

        payload = urlencode(form)
        try:
            proc = subprocess.run(
                [
                    "curl",
                    "-sS",
                    "-X",
                    "POST",
                    "https://discord.com/api/oauth2/token",
                    "-H",
                    "Content-Type: application/x-www-form-urlencoded",
                    "-H",
                    "Accept: application/json",
                    "-H",
                    "User-Agent: 716Stonks-Dashboard/1.0 (+https://716stonks-activity.cfm1.uk)",
                    "--data",
                    payload,
                ],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError((proc.stderr or proc.stdout or "").strip() or f"curl exited {proc.returncode}")
            raw = (proc.stdout or "").strip()
            payload = json.loads(raw) if raw else {}
            if not isinstance(payload, dict):
                raise RuntimeError("Invalid OAuth response payload")
            if "error" in payload:
                return jsonify({"error": f"OAuth token exchange failed: {json.dumps(payload)}"}), 502
        except HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", errors="replace").strip()
            except Exception:
                detail = ""
            msg = f"OAuth token exchange failed: HTTP {exc.code}"
            if detail:
                msg = f"{msg}: {detail}"
            return jsonify({"error": msg}), 502
        except URLError as exc:
            return jsonify({"error": f"OAuth token exchange failed: {exc}"}), 502
        except Exception as exc:
            return jsonify({"error": f"OAuth token exchange failed: {exc}"}), 502

        access_token = str(payload.get("access_token", "") or "").strip()
        if not access_token:
            return jsonify({"error": "OAuth token response missing access_token"}), 502
        return jsonify(
            {
                "access_token": access_token,
                "token_type": str(payload.get("token_type", "") or ""),
                "scope": str(payload.get("scope", "") or ""),
                "expires_in": int(payload.get("expires_in", 0) or 0),
            }
        )

    @app.get("/api/activity/status")
    def api_activity_status():
        user_id_raw = str(request.args.get("user_id", "") or "").strip()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        user_id = int(user_id_raw)
        if user_id <= 0:
            return jsonify({"error": "Invalid user_id"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            if guild_id <= 0:
                return jsonify({"error": "No guild data"}), 404

            user_row = conn.execute(
                """
                SELECT guild_id, user_id, joined_at, bank, networth, owe, display_name, rank
                FROM users
                WHERE guild_id = ? AND user_id = ?
                """,
                (guild_id, user_id),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "User not registered"}), 404

            holdings_rows = conn.execute(
                """
                SELECT symbol, shares
                FROM holdings
                WHERE guild_id = ? AND user_id = ? AND shares > 0
                ORDER BY symbol
                """,
                (guild_id, user_id),
            ).fetchall()
            commodity_rows = conn.execute(
                """
                SELECT c.name AS name, CAST(SUM(uc.quantity) AS INTEGER) AS quantity, c.price
                FROM user_commodities uc
                JOIN commodities c
                  ON c.guild_id = uc.guild_id
                 AND c.name = uc.commodity_name COLLATE NOCASE
                WHERE uc.guild_id = ? AND uc.user_id = ? AND uc.quantity > 0
                GROUP BY c.name, c.price
                ORDER BY c.name
                """,
                (guild_id, user_id),
            ).fetchall()

        user = dict(user_row)
        rank = str(user.get("rank", DEFAULT_RANK) or DEFAULT_RANK)
        lookup = {k.lower(): float(v) for k, v in RANK_INCOME.items()}
        base_income = lookup.get(rank.lower(), float(RANK_INCOME.get(DEFAULT_RANK, 0.0)))
        base_trade_limits = int(_get_config("TRADING_LIMITS"))
        base_commodities_limit = int(_get_config("COMMODITIES_LIMIT"))
        perk_eval_error = ""
        try:
            perk_eval = evaluate_user_perks(
                guild_id=guild_id,
                user_id=user_id,
                base_income=base_income,
                base_trade_limits=base_trade_limits,
                base_networth=float(user.get("networth", 0.0) or 0.0),
                base_commodities_limit=base_commodities_limit,
                base_job_slots=1,
                base_timed_duration=0.0,
                base_chance_roll_bonus=0.0,
            )
        except Exception as exc:
            # Keep Activity status available even if perk backend is temporarily broken
            # (schema mismatch, malformed config/effects, etc.).
            perk_eval_error = str(exc)
            perk_eval = {
                "final": {
                    "income": float(base_income),
                    "trade_limits": float(base_trade_limits),
                    "networth": float(user.get("networth", 0.0) or 0.0),
                    "commodities_limit": float(base_commodities_limit),
                    "job_slots": 1.0,
                    "timed_duration": 0.0,
                    "chance_roll_bonus": 0.0,
                    "slot_bet_multiplier": 1.0,
                    "slot_bet_limit": float(_get_config("SLOT_BET_MAX")),
                    "slot_win_multiplier": 1.0,
                },
                "matched_perks": [],
            }

        period = max(1, int(_get_config("TRADING_LIMITS_PERIOD")))
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        current_tick_raw = _state_get("last_tick")
        try:
            current_tick = max(0, int(current_tick_raw)) if current_tick_raw is not None else 0
        except ValueError:
            current_tick = 0
        bucket = current_tick // period
        used_raw = _state_get(f"trade_used:{guild_id}:{user_id}:{bucket}")
        try:
            used = max(0, int(float(used_raw))) if used_raw is not None else 0
        except ValueError:
            used = 0
        limit = int(perk_eval["final"]["trade_limits"])
        limits_enabled = bool(base_trade_limits > 0 and period > 0)
        remaining = max(0, limit - used) if limits_enabled else 0
        period_minutes = (period * tick_interval) / 60.0

        commodities = []
        commodities_used = 0
        for row in commodity_rows:
            qty = max(0, int(row["quantity"] or 0))
            price = float(row["price"] or 0.0)
            commodities_used += qty
            commodities.append(
                {
                    "name": str(row["name"]),
                    "quantity": qty,
                    "price": price,
                    "value": round(qty * price, 2),
                }
            )

        stocks = [{"symbol": str(r["symbol"]), "shares": int(r["shares"])} for r in holdings_rows]

        matched_perks = []
        for row in list(perk_eval.get("matched_perks", [])):
            matched_perks.append(
                {
                    "name": str(row.get("name", "Unknown")),
                    "display": str(row.get("display", "")).strip(),
                    "stacks": int(row.get("stacks", 1) or 1),
                }
            )

        payload = {
            "user": {
                "user_id": int(user.get("user_id", 0)),
                "display_name": str(user.get("display_name", "") or f"User {user_id}"),
                "rank": rank,
                "joined_at": str(user.get("joined_at", "")),
                "bank": float(user.get("bank", 0.0) or 0.0),
                "networth": float(user.get("networth", 0.0) or 0.0),
                "owe": float(user.get("owe", 0.0) or 0.0),
            },
            "stocks": stocks,
            "commodities": commodities,
            "commodities_used": commodities_used,
            "commodities_limit": int(perk_eval["final"]["commodities_limit"]),
            "trade_limit": {
                "enabled": limits_enabled,
                "limit": limit,
                "used": used,
                "remaining": remaining,
                "period_minutes": period_minutes,
            },
            "perks": matched_perks,
            "perk_summary": {
                "income": float(perk_eval["final"]["income"]),
                "trade_limits": int(perk_eval["final"]["trade_limits"]),
                "networth": float(perk_eval["final"]["networth"]),
                "commodities_limit": int(perk_eval["final"]["commodities_limit"]),
                "job_slots": int(perk_eval["final"]["job_slots"]),
                "timed_duration": float(perk_eval["final"]["timed_duration"]),
                "chance_roll_bonus": float(perk_eval["final"]["chance_roll_bonus"]),
                "slot_bet_multiplier": float(perk_eval["final"]["slot_bet_multiplier"]),
                "slot_bet_limit": float(perk_eval["final"]["slot_bet_limit"]),
                "slot_win_multiplier": float(perk_eval["final"]["slot_win_multiplier"]),
            },
        }
        if perk_eval_error:
            payload["status_warning"] = f"Perk evaluation fallback: {perk_eval_error}"
        return jsonify(payload)

    @app.get("/api/activity/history")
    def api_activity_history():
        user_id_raw = str(request.args.get("user_id", "") or "").strip()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        user_id = int(user_id_raw)
        if user_id <= 0:
            return jsonify({"error": "Invalid user_id"}), 400
        try:
            limit = max(1, min(100, int(request.args.get("limit", 25))))
        except (TypeError, ValueError):
            limit = 25
        try:
            offset = max(0, int(request.args.get("offset", 0)))
        except (TypeError, ValueError):
            offset = 0

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            if guild_id <= 0:
                return jsonify({"error": "No guild data"}), 404
            user_row = conn.execute(
                """
                SELECT 1
                FROM users
                WHERE guild_id = ? AND user_id = ?
                """,
                (guild_id, user_id),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "User not registered"}), 404
            total_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM action_history
                WHERE guild_id = ? AND user_id = ?
                """,
                (guild_id, user_id),
            ).fetchone()
            rows = conn.execute(
                """
                SELECT
                    id,
                    action_type,
                    target_type,
                    target_symbol,
                    quantity,
                    unit_price,
                    total_amount,
                    details,
                    created_at
                FROM action_history
                WHERE guild_id = ? AND user_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (guild_id, user_id, limit, offset),
            ).fetchall()
        total = int(total_row["c"]) if total_row is not None else 0
        debit_actions = {"buy", "loan_repayment"}
        credit_actions = {"sell", "pawn", "loan_approved", "owner_payout"}
        out_rows: list[dict] = []
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
            out_rows.append(item)
        return jsonify(
            {
                "user_id": user_id,
                "limit": limit,
                "offset": offset,
                "total": total,
                "rows": out_rows,
            }
        )

    @app.get("/api/activity/dual/status")
    def api_activity_dual_status():
        user_id_raw = str(request.args.get("user_id", "") or "").strip()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        user_id = int(user_id_raw)
        if user_id <= 0:
            return jsonify({"error": "Invalid user_id"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            if guild_id <= 0:
                return jsonify({"error": "No guild data"}), 404
            user_row = conn.execute(
                """
                SELECT user_id, bank, display_name
                FROM users
                WHERE guild_id = ? AND user_id = ?
                """,
                (guild_id, user_id),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "User not registered"}), 404

        user_game = _dual_find_game_for_user(guild_id, user_id)
        open_lobbies = []
        for g in _dual_all_games(guild_id):
            if str(g.get("status", "lobby")) != "lobby":
                continue
            view = _dual_game_public_view(g)
            open_lobbies.append(view)
        open_lobbies.sort(key=lambda g: str(g.get("updated_at", "")), reverse=True)
        return jsonify(
            {
                "user_id": user_id,
                "bank": float(user_row["bank"] or 0.0),
                "display_name": str(user_row["display_name"] or f"User {user_id}"),
                "current_game": _dual_game_public_view(user_game) if user_game else None,
                "open_lobbies": open_lobbies[:20],
            }
        )

    @app.post("/api/activity/dual/create")
    def api_activity_dual_create():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        display_name = str(data.get("display_name", "") or "").strip()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        user_id = int(user_id_raw)
        if user_id <= 0:
            return jsonify({"error": "Invalid user_id"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        if guild_id <= 0:
            return jsonify({"error": "No guild data"}), 404

        if _dual_find_game_for_user(guild_id, user_id) is not None:
            return jsonify({"error": "You are already in a DUAL game."}), 400
        user = get_user(guild_id, user_id)
        if user is None:
            return jsonify({"error": "User not registered"}), 404

        code = _dual_generate_code(guild_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        game = {
            "code": code,
            "host_user_id": str(user_id),
            "status": "lobby",
            "pot": 0.0,
            "round": 1,
            "min_value": 0,
            "max_value": 100,
            "target": None,
            "winner_user_id": None,
            "created_at": now_iso,
            "updated_at": now_iso,
            "players": {
                str(user_id): {
                    "display_name": display_name or str(user.get("display_name", f"User {user_id}")),
                    "bet": 0.0,
                    "ready": False,
                    "ready_bet": 0.0,
                    "last_guess": None,
                    "last_hint": "",
                    "guessed_round": 0,
                    "joined_at": now_iso,
                }
            },
        }
        _dual_save_game(guild_id, game)
        return jsonify({"ok": True, "game": _dual_game_public_view(game)})

    @app.post("/api/activity/dual/join")
    def api_activity_dual_join():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        display_name = str(data.get("display_name", "") or "").strip()
        code = str(data.get("code", "") or "").strip().upper()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if not code:
            return jsonify({"error": "code is required"}), 400
        user_id = int(user_id_raw)

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        if guild_id <= 0:
            return jsonify({"error": "No guild data"}), 404
        if _dual_find_game_for_user(guild_id, user_id) is not None:
            return jsonify({"error": "You are already in a DUAL game."}), 400

        game = _dual_load_game(guild_id, code)
        if game is None:
            return jsonify({"error": "DUAL room not found."}), 404
        if str(game.get("status", "lobby")) != "lobby":
            return jsonify({"error": "DUAL room is not accepting joins."}), 400

        players = game.get("players")
        if not isinstance(players, dict):
            players = {}
        if str(user_id) in players:
            return jsonify({"error": "You already joined this DUAL room."}), 400
        if len(players) >= 4:
            return jsonify({"error": "DUAL room is full (max 4 players)."}), 400

        user = get_user(guild_id, user_id)
        if user is None:
            return jsonify({"error": "User not registered"}), 404

        now_iso = datetime.now(timezone.utc).isoformat()
        players[str(user_id)] = {
            "display_name": display_name or str(user.get("display_name", f"User {user_id}")),
            "bet": 0.0,
            "ready": False,
            "ready_bet": 0.0,
            "last_guess": None,
            "last_hint": "",
            "guessed_round": 0,
            "joined_at": now_iso,
        }
        game["players"] = players
        game["pot"] = float(money(float(game.get("pot", 0.0) or 0.0)))
        _dual_save_game(guild_id, game)
        return jsonify({"ok": True, "game": _dual_game_public_view(game)})

    @app.post("/api/activity/dual/ready")
    def api_activity_dual_ready():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        code = str(data.get("code", "") or "").strip().upper()
        try:
            bet = max(0.01, float(data.get("bet", 0.0)))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid bet"}), 400
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if not code:
            return jsonify({"error": "code is required"}), 400
        user_id = int(user_id_raw)

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        game = _dual_load_game(guild_id, code)
        if game is None:
            return jsonify({"error": "DUAL room not found."}), 404
        if str(game.get("status", "lobby")) != "lobby":
            return jsonify({"error": "DUAL is not in lobby state."}), 400
        players = game.get("players")
        if not isinstance(players, dict) or str(user_id) not in players:
            return jsonify({"error": "You are not in this DUAL room."}), 403

        player = players[str(user_id)]
        player["ready"] = True
        player["ready_bet"] = float(money(bet))
        player["bet"] = float(money(bet))
        game["players"] = players

        ready_count = 0
        for row in players.values():
            if not isinstance(row, dict):
                continue
            if bool(row.get("ready", False)):
                ready_count += 1

        if len(players) >= 2 and ready_count == len(players):
            deductions: list[tuple[int, float]] = []
            for uid, row in players.items():
                if not isinstance(row, dict):
                    continue
                try:
                    pid = int(uid)
                except (TypeError, ValueError):
                    continue
                stake = float(row.get("ready_bet", 0.0) or 0.0)
                u = get_user(guild_id, pid)
                if u is None:
                    return jsonify({"error": "A player is no longer registered."}), 400
                bank = float(u.get("bank", 0.0) or 0.0)
                if bank < stake:
                    row["ready"] = False
                    row["ready_bet"] = 0.0
                    row["bet"] = 0.0
                    game["players"] = players
                    _dual_save_game(guild_id, game)
                    return jsonify(
                        {
                            "error": f"{row.get('display_name', f'User {uid}')} has insufficient funds and was un-readied."
                        }
                    ), 400
                deductions.append((pid, stake))

            total_pot = 0.0
            now_iso = datetime.now(timezone.utc).isoformat()
            for pid, stake in deductions:
                u = get_user(guild_id, pid)
                if u is None:
                    continue
                update_user_bank(guild_id, pid, float(u.get("bank", 0.0) or 0.0) - stake)
                total_pot += stake
                add_action_history(
                    guild_id=guild_id,
                    user_id=pid,
                    action_type="dual_bet",
                    target_type="game",
                    target_symbol=code,
                    quantity=1.0,
                    unit_price=float(stake),
                    total_amount=float(stake),
                    details="source=activity_dual_ready",
                    created_at=now_iso,
                )

            min_value = int(game.get("min_value", 0) or 0)
            max_value = int(game.get("max_value", 100) or 100)
            if min_value > max_value:
                min_value, max_value = max_value, min_value
            target = random.randint(min_value, max_value)
            game["status"] = "active"
            game["pot"] = float(money(total_pot))
            game["target"] = int(target)
            game["round"] = 1
            game["winner_user_id"] = None
            for row in players.values():
                if not isinstance(row, dict):
                    continue
                row["guessed_round"] = 0
                row["last_guess"] = None
                row["last_hint"] = ""
        else:
            game["pot"] = float(money(0.0))

        _dual_save_game(guild_id, game)
        return jsonify({"ok": True, "game": _dual_game_public_view(game)})

    @app.post("/api/activity/dual/start")
    def api_activity_dual_start():
        return jsonify({"error": "Manual start is disabled. Players must ready up."}), 400

    @app.post("/api/activity/dual/guess")
    def api_activity_dual_guess():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        code = str(data.get("code", "") or "").strip().upper()
        try:
            guess = int(data.get("guess"))
        except (TypeError, ValueError):
            return jsonify({"error": "guess must be an integer"}), 400
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if not code:
            return jsonify({"error": "code is required"}), 400
        user_id = int(user_id_raw)

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        game = _dual_load_game(guild_id, code)
        if game is None:
            return jsonify({"error": "DUAL room not found."}), 404
        if str(game.get("status", "")) != "active":
            return jsonify({"error": "DUAL is not active."}), 400
        players = game.get("players")
        if not isinstance(players, dict) or str(user_id) not in players:
            return jsonify({"error": "You are not in this DUAL room."}), 403

        min_value = int(game.get("min_value", 0) or 0)
        max_value = int(game.get("max_value", 100) or 100)
        if guess < min_value or guess > max_value:
            return jsonify({"error": f"Guess must be between {min_value} and {max_value}."}), 400
        target = int(game.get("target", min_value))
        round_no = max(1, int(game.get("round", 1) or 1))
        player = players[str(user_id)]
        if int(player.get("guessed_round", 0) or 0) >= round_no:
            return jsonify({"error": "You already guessed this round."}), 400

        if guess == target:
            hint = "exact"
            winner_user_id = str(user_id)
            pot = float(game.get("pot", 0.0) or 0.0)
            winner_user = get_user(guild_id, user_id)
            if winner_user is not None and pot > 0:
                new_bank = float(winner_user.get("bank", 0.0) or 0.0) + pot
                update_user_bank(guild_id, user_id, new_bank)
                add_action_history(
                    guild_id=guild_id,
                    user_id=user_id,
                    action_type="dual_win",
                    target_type="game",
                    target_symbol=code,
                    quantity=1.0,
                    unit_price=float(pot),
                    total_amount=float(pot),
                    details=f"players={len(players)};target={target};round={round_no}",
                    created_at=datetime.now(timezone.utc).isoformat(),
                )
            game["status"] = "finished"
            game["winner_user_id"] = winner_user_id
        elif guess < target:
            hint = "too low"
        else:
            hint = "too high"

        player["last_guess"] = int(guess)
        player["last_hint"] = hint
        player["guessed_round"] = round_no

        if str(game.get("status")) == "active":
            all_guessed = True
            for row in players.values():
                if not isinstance(row, dict) or int(row.get("guessed_round", 0) or 0) < round_no:
                    all_guessed = False
                    break
            if all_guessed:
                game["round"] = round_no + 1

        game["players"] = players
        _dual_save_game(guild_id, game)
        return jsonify(
            {
                "ok": True,
                "hint": hint,
                "game": _dual_game_public_view(game),
            }
        )

    @app.post("/api/activity/dual/cancel")
    def api_activity_dual_cancel():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        code = str(data.get("code", "") or "").strip().upper()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if not code:
            return jsonify({"error": "code is required"}), 400
        user_id = int(user_id_raw)

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        game = _dual_load_game(guild_id, code)
        if game is None:
            return jsonify({"error": "DUAL room not found."}), 404
        if str(game.get("host_user_id", "")) != str(user_id):
            return jsonify({"error": "Only the host can cancel DUAL."}), 403
        if str(game.get("status", "lobby")) != "lobby":
            return jsonify({"error": "Only lobby DUAL rooms can be canceled."}), 400
        players = game.get("players")
        if isinstance(players, dict):
            for uid, row in players.items():
                if not isinstance(row, dict):
                    continue
                try:
                    pid = int(uid)
                except (TypeError, ValueError):
                    continue
                stake = float(row.get("bet", 0.0) or 0.0)
                if stake <= 0:
                    continue
                u = get_user(guild_id, pid)
                if u is None:
                    continue
                update_user_bank(guild_id, pid, float(u.get("bank", 0.0) or 0.0) + stake)
                add_action_history(
                    guild_id=guild_id,
                    user_id=pid,
                    action_type="dual_refund",
                    target_type="game",
                    target_symbol=code,
                    quantity=1.0,
                    unit_price=float(stake),
                    total_amount=float(stake),
                    details="source=activity_dual_cancel",
                    created_at=datetime.now(timezone.utc).isoformat(),
                )
        _state_delete(_dual_game_key(guild_id, code))
        return jsonify({"ok": True})

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
            item["image_url"] = _canonicalize_media_url(item.get("image_url", ""))
            item["tags"] = tag_map.get(str(item.get("name", "")).lower(), [])
            out.append(item)
        return jsonify({"commodities": out})

    @app.get("/api/shop")
    def api_shop():
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        bucket, items, available = get_shop_items(guild_id)
        out_items: list[dict] = []
        for row in items:
            item = dict(row)
            item["image_url"] = _canonicalize_media_url(item.get("image_url", ""))
            out_items.append(item)
        return jsonify(
            {
                "guild_id": guild_id,
                "bucket": bucket,
                "items": out_items,
                "available": available,
            }
        )

    @app.post("/api/activity/shop/buy")
    def api_activity_shop_buy():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        commodity_name = str(data.get("name", "") or "").strip()
        quantity = 1
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if not commodity_name:
            return jsonify({"error": "name is required"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        if guild_id <= 0:
            return jsonify({"error": "No guild data"}), 404

        user_id = int(user_id_raw)
        user = get_user(guild_id, user_id)
        if user is None:
            return jsonify({"error": "User not registered"}), 404
        before_perks = _matched_perk_map_for_user(guild_id, user_id)
        app.logger.warning(
            "[activity-shop] buy start guild=%s user=%s item=%s before_perks=%s",
            guild_id,
            user_id,
            commodity_name,
            sorted(before_perks.values()),
        )

        _bucket, items, _available = get_shop_items(guild_id)
        shop_item = next(
            (it for it in items if str(it.get("name", "")).strip().lower() == commodity_name.lower()),
            None,
        )
        if shop_item is None:
            return jsonify({"error": f"`{commodity_name}` is not in current shop rotation."}), 400
        if int(shop_item.get("in_stock", 0) or 0) != 1:
            return jsonify({"error": f"`{commodity_name}` is currently sold out."}), 400

        commodity = get_commodity(guild_id, commodity_name)
        if commodity is None:
            return jsonify({"error": f"Commodity `{commodity_name}` not found."}), 404
        if int(commodity.get("enabled", 1) or 0) != 1:
            return jsonify({"error": f"Commodity `{commodity_name}` is currently disabled."}), 400

        base_limit = int(_get_config("COMMODITIES_LIMIT"))
        evaluated = evaluate_user_perks(
            guild_id=guild_id,
            user_id=user_id,
            base_income=0.0,
            base_trade_limits=0,
            base_networth=0.0,
            base_commodities_limit=base_limit,
            base_job_slots=1,
        )
        limit = int(evaluated["final"]["commodities_limit"])
        holdings = get_user_commodities(guild_id, user_id)
        used = sum(max(0, int(row.get("quantity", 0))) for row in holdings)
        if limit > 0 and used + quantity > limit:
            remaining = max(0, limit - used)
            return jsonify({"error": f"Commodity limit reached. Remaining slots: {remaining}/{limit} units."}), 400

        price = float(commodity.get("price", 0.0))
        total_cost = round(price * quantity, 2)
        user_bank = float(user.get("bank", 0.0))
        if user_bank < total_cost:
            return jsonify({"error": f"Not enough funds. Need ${total_cost:.2f}, you have ${user_bank:.2f}."}), 400

        new_bank = user_bank - total_cost
        update_user_bank(guild_id, user_id, new_bank)
        upsert_user_commodity(guild_id, user_id, str(commodity.get("name", commodity_name)), quantity)
        new_networth = float(user.get("networth", 0.0)) + total_cost
        update_user_networth(guild_id, user_id, new_networth)
        add_action_history(
            guild_id=guild_id,
            user_id=user_id,
            action_type="buy",
            target_type="commodity",
            target_symbol=str(commodity.get("name", commodity_name)),
            quantity=float(quantity),
            unit_price=price,
            total_amount=total_cost,
            details="source=activity_shop",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        # Shop item is one-time available per tick bucket.
        _bucket2, _items2, _ = get_shop_items(guild_id)
        set_state_value(sold_key(guild_id, _bucket2, str(commodity.get("name", commodity_name))), "1")

        # Keep stored networth in sync with canonical calculation.
        recalc_user_networth(guild_id, user_id)
        after_perks = _matched_perk_map_for_user(guild_id, user_id)
        newly_matched_ids = [
            perk_id
            for perk_id in sorted(after_perks.keys())
            if perk_id not in before_perks
        ]
        # Force a re-announce for perks newly matched by this activity buy
        # so stale perk_active state cannot suppress expected user mentions.
        for perk_id in newly_matched_ids:
            set_state_value(f"perk_active:{guild_id}:{user_id}:{perk_id}", "0")
        active_state_ids = _active_perk_ids_from_state(guild_id, user_id)
        activated_perks = [
            after_perks[perk_id]
            for perk_id in sorted(after_perks.keys())
            if perk_id not in active_state_ids
        ]
        queue_key = f"perk_check_queue:{guild_id}:{user_id}:{int(time.time() * 1000)}"
        _state_set(queue_key, "1")
        set_state_value(queue_key, "1")
        active_key = f"user_last_active:{guild_id}:{user_id}"
        now_ts = str(time.time())
        _state_set(active_key, now_ts)
        set_state_value(active_key, now_ts)
        app.logger.warning(
            "[activity-shop] buy done guild=%s user=%s item=%s queue=%s activated=%s newly_matched_ids=%s active_state_ids=%s after_perks=%s",
            guild_id,
            user_id,
            commodity_name,
            queue_key,
            activated_perks,
            newly_matched_ids,
            sorted(active_state_ids),
            sorted(after_perks.values()),
        )

        return jsonify(
            {
                "ok": True,
                "name": str(commodity.get("name", commodity_name)),
                "quantity": int(quantity),
                "unit_price": price,
                "total_cost": total_cost,
                "bank": float(new_bank),
                "activated_perks": activated_perks,
            }
        )

    @app.post("/api/activity/shop/sell")
    def api_activity_shop_sell():
        data = request.get_json(silent=True) or {}
        user_id_raw = str(data.get("user_id", "") or "").strip()
        commodity_name = str(data.get("name", "") or "").strip()
        if user_id_raw.startswith("+"):
            user_id_raw = user_id_raw[1:]
        if not user_id_raw.isdigit():
            return jsonify({"error": "user_id is required"}), 400
        if not commodity_name:
            return jsonify({"error": "name is required"}), 400

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
        if guild_id <= 0:
            return jsonify({"error": "No guild data"}), 404

        user_id = int(user_id_raw)
        user = get_user(guild_id, user_id)
        if user is None:
            return jsonify({"error": "User not registered"}), 404
        commodity = get_commodity(guild_id, commodity_name)
        if commodity is None:
            return jsonify({"error": f"Commodity `{commodity_name}` not found."}), 404

        before_perks = _matched_perk_map_for_user(guild_id, user_id)
        app.logger.warning(
            "[activity-shop] sell start guild=%s user=%s item=%s before_perks=%s",
            guild_id,
            user_id,
            commodity_name,
            sorted(before_perks.values()),
        )

        holdings = get_user_commodities(guild_id, user_id)
        match = next(
            (row for row in holdings if str(row.get("name", "")).lower() == str(commodity.get("name", "")).lower()),
            None,
        )
        owned_qty = int(match.get("quantity", 0)) if match is not None else 0
        if owned_qty < 1:
            return jsonify({"error": f"You do not own `{commodity.get('name', commodity_name)}`."}), 400

        rate_pct = max(0.0, min(100.0, float(_get_config("PAWN_SELL_RATE"))))
        rate = rate_pct / 100.0
        price = float(commodity.get("price", 0.0))
        payout = round(price * rate, 2)

        new_bank = float(user.get("bank", 0.0)) + payout
        update_user_bank(guild_id, user_id, new_bank)
        upsert_user_commodity(guild_id, user_id, str(commodity.get("name", commodity_name)), -1)
        networth = recalc_user_networth(guild_id, user_id)

        add_action_history(
            guild_id=guild_id,
            user_id=user_id,
            action_type="pawn",
            target_type="commodity",
            target_symbol=str(commodity.get("name", commodity_name)),
            quantity=1.0,
            unit_price=price * rate,
            total_amount=payout,
            details=f"market_price={price:.2f};pawn_rate={rate_pct:.2f}%;source=activity_shop",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        queue_key = f"perk_check_queue:{guild_id}:{user_id}:{int(time.time() * 1000)}"
        _state_set(queue_key, "1")
        set_state_value(queue_key, "1")
        active_key = f"user_last_active:{guild_id}:{user_id}"
        now_ts = str(time.time())
        _state_set(active_key, now_ts)
        set_state_value(active_key, now_ts)

        after_perks = _matched_perk_map_for_user(guild_id, user_id)
        app.logger.warning(
            "[activity-shop] sell done guild=%s user=%s item=%s queue=%s after_perks=%s",
            guild_id,
            user_id,
            commodity_name,
            queue_key,
            sorted(after_perks.values()),
        )

        return jsonify(
            {
                "ok": True,
                "name": str(commodity.get("name", commodity_name)),
                "quantity": 1,
                "unit_price": price * rate,
                "total_gain": payout,
                "bank": float(new_bank),
                "networth": float(networth),
            }
        )

    @app.get("/api/activity/image-proxy")
    def api_activity_image_proxy():
        raw_url = str(request.args.get("url", "") or "").strip()
        if not raw_url:
            return jsonify({"error": "url is required"}), 400
        parsed = urlparse(raw_url)

        # Handle local uploaded media directly from disk.
        local_media_path = ""
        if raw_url.startswith("/media/uploads/"):
            local_media_path = raw_url
        elif parsed.scheme in {"http", "https"} and parsed.path.startswith("/media/uploads/"):
            local_media_path = parsed.path

        if local_media_path:
            filename = Path(local_media_path).name
            if not filename:
                return jsonify({"error": "Invalid media path"}), 400
            path = _uploads_dir() / filename
            if not path.exists() or not path.is_file():
                return jsonify({"error": "Not found"}), 404
            ext = path.suffix.lower()
            mimetype = {
                ".webp": "image/webp",
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".gif": "image/gif",
            }.get(ext, "application/octet-stream")
            return send_file(path, mimetype=mimetype)

        if parsed.scheme not in {"http", "https"}:
            return jsonify({"error": "Invalid URL scheme"}), 400
        if not parsed.netloc:
            return jsonify({"error": "Invalid URL host"}), 400

        req = Request(
            raw_url,
            headers={
                "User-Agent": "716Stonks-ActivityImageProxy/1.0",
                "Accept": "image/*",
            },
        )
        try:
            with urlopen(req, timeout=10) as resp:
                content_type = str(resp.headers.get("Content-Type", "")).split(";")[0].strip().lower()
                if not content_type.startswith("image/"):
                    return jsonify({"error": "Target is not an image"}), 400
                data = resp.read(2 * 1024 * 1024 + 1)
                if len(data) > 2 * 1024 * 1024:
                    return jsonify({"error": "Image too large"}), 413
        except HTTPError as exc:
            return jsonify({"error": f"Upstream image fetch failed: HTTP {exc.code}"}), 502
        except URLError as exc:
            return jsonify({"error": f"Upstream image fetch failed: {exc.reason}"}), 502
        except Exception as exc:
            return jsonify({"error": f"Upstream image fetch failed: {exc}"}), 502

        return send_file(io.BytesIO(data), mimetype=content_type)

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
        item["image_url"] = _canonicalize_media_url(item.get("image_url", ""))
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

    @app.get("/api/properties")
    def api_properties():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT id, guild_id, name, description, image_url, enabled, max_level, buy_price,
                       level_costs_json, level_effects_json, ascension_cost, ascension_effects_json, updated_at
                FROM properties
                ORDER BY name ASC
                """
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["image_url"] = _canonicalize_media_url(item.get("image_url", ""))
            max_level = max(1, int(item.get("max_level", 5) or 5))
            try:
                costs = json.loads(str(item.get("level_costs_json", "[]") or "[]"))
            except json.JSONDecodeError:
                costs = []
            if not isinstance(costs, list):
                costs = []
            normalized_costs = []
            for i in range(max_level):
                raw = costs[i] if i < len(costs) else 0
                try:
                    normalized_costs.append(max(0.0, float(raw)))
                except (TypeError, ValueError):
                    normalized_costs.append(0.0)
            item["level_costs"] = normalized_costs
            try:
                effects_raw = json.loads(str(item.get("level_effects_json", "[]") or "[]"))
            except json.JSONDecodeError:
                effects_raw = []
            if not isinstance(effects_raw, list):
                effects_raw = []
            level_effects: list[list[dict]] = [[] for _ in range(max_level)]
            for i in range(min(max_level, len(effects_raw))):
                level_effects[i] = _parse_effects_json(effects_raw[i])
            item["level_effects"] = level_effects
            item["ascension_effects"] = _parse_effects_json(item.get("ascension_effects_json", "[]"))
            items.append(item)
        return jsonify({"properties": items})

    @app.get("/api/property/<int:property_id>")
    def api_property(property_id: int):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT id, guild_id, name, description, image_url, enabled, max_level, buy_price,
                       level_costs_json, level_effects_json, ascension_cost, ascension_effects_json, updated_at
                FROM properties
                WHERE id = ?
                """,
                (property_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Property not found"}), 404
            owner_rows = conn.execute(
                """
                SELECT up.user_id, COALESCE(u.display_name, '') AS display_name, up.level, up.ascended
                FROM user_properties up
                LEFT JOIN users u
                  ON u.guild_id = up.guild_id
                 AND u.user_id = up.user_id
                WHERE up.guild_id = ? AND up.property_id = ? AND (up.level > 0 OR up.ascended > 0)
                ORDER BY up.level DESC, up.ascended DESC, up.user_id ASC
                """,
                (int(row["guild_id"]), property_id),
            ).fetchall()
        item = dict(row)
        item["image_url"] = _canonicalize_media_url(item.get("image_url", ""))
        max_level = max(1, int(item.get("max_level", 5) or 5))
        try:
            costs = json.loads(str(item.get("level_costs_json", "[]") or "[]"))
        except json.JSONDecodeError:
            costs = []
        if not isinstance(costs, list):
            costs = []
        normalized_costs = []
        for i in range(max_level):
            raw = costs[i] if i < len(costs) else 0
            try:
                normalized_costs.append(max(0.0, float(raw)))
            except (TypeError, ValueError):
                normalized_costs.append(0.0)
        item["level_costs"] = normalized_costs
        try:
            effects_raw = json.loads(str(item.get("level_effects_json", "[]") or "[]"))
        except json.JSONDecodeError:
            effects_raw = []
        if not isinstance(effects_raw, list):
            effects_raw = []
        level_effects: list[list[dict]] = [[] for _ in range(max_level)]
        for i in range(min(max_level, len(effects_raw))):
            level_effects[i] = _parse_effects_json(effects_raw[i])
        item["level_effects"] = level_effects
        item["ascension_effects"] = _parse_effects_json(item.get("ascension_effects_json", "[]"))
        owners = []
        for r in owner_rows:
            uid = str(r["user_id"])
            display_name = str(r["display_name"] or "").strip() or f"User {uid}"
            owners.append(
                {
                    "user_id": uid,
                    "display_name": display_name,
                    "level": int(r["level"] or 0),
                    "ascended": int(r["ascended"] or 0),
                }
            )
        return jsonify({"property": item, "owners": owners})

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
                active_by_guild[guild_id] = active_user_ids_last_day(
                    guild_id,
                    connection_factory=_connect,
                )
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
        bonus_raw = _state_get(f"daily_networth_bonus:{guild_id}:{user_id}")
        try:
            player["daily_networth_bonus"] = float(bonus_raw) if bonus_raw is not None else 0.0
        except (TypeError, ValueError):
            player["daily_networth_bonus"] = 0.0

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

    @app.get("/api/player/<int:user_id>/properties")
    def api_player_properties(user_id: int):
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
                    p.id,
                    p.name,
                    p.enabled,
                    p.max_level,
                    COALESCE(up.level, 0) AS level,
                    COALESCE(up.ascended, 0) AS ascended,
                    COALESCE(up.ascended_count, 0) AS ascended_count
                FROM properties p
                LEFT JOIN user_properties up
                  ON up.guild_id = p.guild_id
                 AND up.user_id = ?
                 AND up.property_id = p.id
                WHERE p.guild_id = ?
                ORDER BY p.name ASC
                """,
                (user_id, guild_id),
            ).fetchall()
        props = []
        for r in rows:
            item = dict(r)
            item["id"] = int(item["id"])
            item["enabled"] = int(item.get("enabled") or 0)
            item["max_level"] = max(1, int(item.get("max_level") or 1))
            item["level"] = max(0, int(item.get("level") or 0))
            item["ascended"] = int(item.get("ascended") or 0)
            item["ascended_count"] = max(0, int(item.get("ascended_count") or 0))
            props.append(item)
        return jsonify({"properties": props})

    @app.post("/api/player/<int:user_id>/properties/set")
    def api_player_set_property_state(user_id: int):
        data = request.get_json(silent=True) or {}
        try:
            property_id = int(data.get("property_id", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid property_id"}), 400
        try:
            level = max(0, int(float(data.get("level", 0))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid level"}), 400
        ascended_raw = data.get("ascended", 0)
        ascended = 1 if str(ascended_raw).strip().lower() in {"1", "true", "yes", "on"} else 0
        try:
            ascended_count = max(0, int(float(data.get("ascended_count", 0))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid ascended_count"}), 400

        if property_id <= 0:
            return jsonify({"error": "property_id is required"}), 400

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
            prop_row = conn.execute(
                """
                SELECT id, max_level
                FROM properties
                WHERE guild_id = ? AND id = ?
                """,
                (guild_id, property_id),
            ).fetchone()
            if prop_row is None:
                return jsonify({"error": "Property not found"}), 404

            max_level = max(1, int(prop_row["max_level"] or 1))
            level = max(0, min(max_level, level))
            if ascended <= 0:
                ascended_count = 0

            if level <= 0 and ascended <= 0 and ascended_count <= 0:
                conn.execute(
                    """
                    DELETE FROM user_properties
                    WHERE guild_id = ? AND user_id = ? AND property_id = ?
                    """,
                    (guild_id, user_id, property_id),
                )
            else:
                now = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO user_properties (
                        guild_id, user_id, property_id, level, ascended, ascended_count, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, property_id)
                    DO UPDATE SET
                        level = excluded.level,
                        ascended = excluded.ascended,
                        ascended_count = excluded.ascended_count,
                        updated_at = excluded.updated_at
                    """,
                    (guild_id, user_id, property_id, level, ascended, ascended_count, now),
                )
        return jsonify(
            {
                "ok": True,
                "property_id": property_id,
                "level": level,
                "ascended": ascended,
                "ascended_count": ascended_count,
            }
        )

    @app.get("/api/player/<int:user_id>/perks")
    def api_player_perks(user_id: int):
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
            perks_rows = conn.execute(
                """
                SELECT id, name, enabled
                FROM perks
                WHERE guild_id = ?
                ORDER BY name ASC
                """,
                (guild_id,),
            ).fetchall()
            override_rows = conn.execute(
                """
                SELECT key, value
                FROM app_state
                WHERE key LIKE ?
                """,
                (f"perk_user_override:{guild_id}:{user_id}:%",),
            ).fetchall()

        override_by_id: dict[int, str] = {}
        for row in override_rows:
            key = str(row["key"] or "")
            value = str(row["value"] or "").strip().lower()
            if value not in {"auto", "on", "off"}:
                continue
            try:
                pid = int(key.rsplit(":", 1)[-1])
            except (TypeError, ValueError):
                continue
            if pid > 0:
                override_by_id[pid] = value

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
        matched_ids = {
            int(m.get("perk_id", 0))
            for m in matched
            if int(m.get("perk_id", 0)) > 0
        }
        all_perks = []
        for r in perks_rows:
            item = dict(r)
            pid = int(item.get("id") or 0)
            item["id"] = pid
            item["enabled"] = int(item.get("enabled") or 0)
            item["matched"] = pid in matched_ids
            item["override_mode"] = override_by_id.get(pid, "auto")
            all_perks.append(item)

        return jsonify(
            {
                "user_id": str(user_id),
                "display_name": display_name,
                "rank": rank,
                "summary": {
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
                    "base_slot_bet_multiplier": float(result["base"]["slot_bet_multiplier"]),
                    "final_slot_bet_multiplier": float(result["final"]["slot_bet_multiplier"]),
                    "base_slot_bet_limit": float(result["base"]["slot_bet_limit"]),
                    "final_slot_bet_limit": float(result["final"]["slot_bet_limit"]),
                    "base_slot_win_multiplier": float(result["base"]["slot_win_multiplier"]),
                    "final_slot_win_multiplier": float(result["final"]["slot_win_multiplier"]),
                },
                "matched_perks": matched,
                "all_perks": all_perks,
            }
        )

    @app.post("/api/player/<int:user_id>/perks/<int:perk_id>/override")
    def api_player_perk_override(user_id: int, perk_id: int):
        data = request.get_json(silent=True) or {}
        mode = str(data.get("mode", "auto") or "auto").strip().lower()
        if mode not in {"auto", "on", "off"}:
            return jsonify({"error": "mode must be auto, on, or off"}), 400

        with _connect() as conn:
            user_row = conn.execute(
                """
                SELECT guild_id
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if user_row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(user_row["guild_id"])
            perk_row = conn.execute(
                """
                SELECT id
                FROM perks
                WHERE guild_id = ? AND id = ?
                """,
                (guild_id, perk_id),
            ).fetchone()
            if perk_row is None:
                return jsonify({"error": "Perk not found"}), 404

        key = f"perk_user_override:{guild_id}:{user_id}:{perk_id}"
        if mode == "auto":
            with _connect() as conn:
                conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
        else:
            _state_set(key, mode)
        return jsonify({"ok": True, "mode": mode})

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
                SELECT name
                FROM commodities
                WHERE guild_id = ? AND name = ? COLLATE NOCASE
                """,
                (guild_id, commodity_name),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Commodity not found"}), 404
            canonical_name = str(exists["name"])

            if quantity <= 0:
                conn.execute(
                    """
                    DELETE FROM user_commodities
                    WHERE guild_id = ? AND user_id = ? AND commodity_name = ? COLLATE NOCASE
                    """,
                    (guild_id, user_id, canonical_name),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO user_commodities (guild_id, user_id, commodity_name, quantity)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, commodity_name)
                    DO UPDATE SET quantity = excluded.quantity
                    """,
                    (guild_id, user_id, canonical_name, quantity),
                )

            # Keep networth in sync with inventory edits.
            networth_row = conn.execute(
                """
                SELECT COALESCE(SUM(uc.quantity * c.price), 0.0) AS total
                FROM user_commodities uc
                JOIN commodities c
                  ON c.guild_id = uc.guild_id
                 AND c.name = uc.commodity_name COLLATE NOCASE
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

        return jsonify({"ok": True, "commodity_name": canonical_name, "quantity": quantity})

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
                "base_slot_bet_multiplier": float(result["base"]["slot_bet_multiplier"]),
                "final_slot_bet_multiplier": float(result["final"]["slot_bet_multiplier"]),
                "base_slot_bet_limit": float(result["base"]["slot_bet_limit"]),
                "final_slot_bet_limit": float(result["final"]["slot_bet_limit"]),
                "base_slot_win_multiplier": float(result["base"]["slot_win_multiplier"]),
                "final_slot_win_multiplier": float(result["final"]["slot_win_multiplier"]),
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

    @app.get("/api/slots")
    def api_slots():
        return jsonify({"slot_settings": _slot_config_payload()})

    @app.post("/api/slots/update")
    def api_slots_update():
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        for key in (
            "SLOT_BET_MIN",
            "SLOT_BET_MAX",
            "SLOT_MAX_SPINS",
            "SLOT_HOURLY_SPIN_LIMIT",
            "SLOT_PAIR_PAYOUT_BASE",
            "SLOT_TRIPLE_MULT_CHERRY",
            "SLOT_TRIPLE_MULT_LEMON",
            "SLOT_TRIPLE_MULT_WATERMELON",
            "SLOT_TRIPLE_MULT_BELL",
            "SLOT_TRIPLE_MULT_STAR",
            "SLOT_TRIPLE_MULT_SEVEN",
            "SLOT_TRIPLE_MULT_DIAMOND",
        ):
            if key in data:
                updates[key] = data.get(key)
        if not updates:
            return jsonify({"error": "No slot settings provided."}), 400
        normalized: dict[str, object] = {}
        for key, raw in updates.items():
            try:
                normalized[key] = _set_config(key, raw)
            except Exception as exc:
                return jsonify({"error": f"Invalid value for {key}: {exc}"}), 400

        low = float(_get_config("SLOT_BET_MIN"))
        high = float(_get_config("SLOT_BET_MAX"))
        if low > high:
            _set_config("SLOT_BET_MIN", high)
            _set_config("SLOT_BET_MAX", low)

        return jsonify({"ok": True, "slot_settings": _slot_config_payload()})

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
        news = []
        for r in rows:
            item = dict(r)
            item["image_url"] = _canonicalize_media_url(item.get("image_url", ""))
            news.append(item)
        return jsonify({"guild_id": guild_id, "news": news})

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
                if _has_disabled_perk_effect_target(parsed):
                    return jsonify({"error": "One or more perk effect targets are disabled"}), 400
            except json.JSONDecodeError:
                return jsonify({"error": "Invalid perk_effects_json"}), 400
            updates["perk_effects_json"] = raw_json
        tags = _parse_tags(data.get("tags", "")) if tags_in_payload else []

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
            next_name = str(updates.get("name", old_name)).strip()
            if not next_name:
                return jsonify({"error": "name cannot be empty"}), 400
            renamed = next_name.lower() != old_name.lower()
            if renamed:
                existing = conn.execute(
                    """
                    SELECT 1
                    FROM commodities
                    WHERE guild_id = ? AND name = ? COLLATE NOCASE
                    """,
                    (guild_id, next_name),
                ).fetchone()
                if existing is not None:
                    return jsonify({"error": f"Commodity `{next_name}` already exists"}), 409
            if "image_url" in updates:
                try:
                    updates["image_url"] = _normalize_image_url_for_storage(conn, updates.get("image_url", ""))
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400
            if renamed:
                # Allow parent/child commodity name updates in one transaction.
                conn.execute("PRAGMA defer_foreign_keys = ON;")
            if updates:
                set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                values = list(updates.values())
                values.extend([guild_id, old_name])
                try:
                    conn.execute(
                        f"UPDATE commodities SET {set_clause} WHERE guild_id = ? AND name = ? COLLATE NOCASE",
                        values,
                    )
                except sqlite3.IntegrityError:
                    return jsonify({"error": f"Commodity `{next_name}` already exists"}), 409
            if renamed:
                conn.execute(
                    """
                    UPDATE user_commodities
                    SET commodity_name = ?
                    WHERE guild_id = ? AND commodity_name = ? COLLATE NOCASE
                    """,
                    (next_name, guild_id, old_name),
                )
                conn.execute(
                    """
                    UPDATE commodity_tags
                    SET commodity_name = ?
                    WHERE guild_id = ? AND commodity_name = ? COLLATE NOCASE
                    """,
                    (next_name, guild_id, old_name),
                )
                conn.execute(
                    """
                    UPDATE perk_requirements
                    SET commodity_name = ?
                    WHERE commodity_name = ? COLLATE NOCASE
                      AND perk_id IN (SELECT id FROM perks WHERE guild_id = ?)
                    """,
                    (next_name, old_name, guild_id),
                )
            target_name = next_name if renamed else old_name
            if tags_in_payload:
                conn.execute(
                    """
                    DELETE FROM commodity_tags
                    WHERE guild_id = ? AND commodity_name = ? COLLATE NOCASE
                    """,
                    (guild_id, target_name),
                )
                for tag in tags:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO commodity_tags (guild_id, commodity_name, tag)
                        VALUES (?, ?, ?)
                        """,
                        (guild_id, target_name, tag),
                    )
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "name": next_name})

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
            if _has_disabled_perk_effect_target(parsed_perk):
                return jsonify({"error": "One or more perk effect targets are disabled"}), 400
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

    @app.post("/api/property")
    def api_property_create():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        description = str(data.get("description", "")).strip()
        image_url = str(data.get("image_url", "")).strip()
        enabled = 1 if int(data.get("enabled", 1) or 1) > 0 else 0
        max_level = max(1, min(10, int(float(data.get("max_level", 5) or 5))))
        buy_price = max(0.0, float(data.get("buy_price", 0.0) or 0.0))
        ascension_cost = max(0.0, float(data.get("ascension_cost", 0.0) or 0.0))
        level_costs = data.get("level_costs", [0, 0, 0, 0, 0])
        if not isinstance(level_costs, list):
            level_costs = []
        normalized_costs: list[float] = []
        for i in range(max_level):
            raw = level_costs[i] if i < len(level_costs) else 0
            try:
                normalized_costs.append(max(0.0, float(raw)))
            except (TypeError, ValueError):
                normalized_costs.append(0.0)
        level_effects = data.get("level_effects", [])
        if not isinstance(level_effects, list):
            level_effects = []
        normalized_effects: list[list[dict]] = []
        for i in range(max_level):
            raw = level_effects[i] if i < len(level_effects) else []
            normalized_effects.append(_parse_effects_json(raw))
        ascension_effects = _parse_effects_json(data.get("ascension_effects", []))
        now = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            try:
                image_url, image_status = _normalize_image_url_with_status(conn, image_url)
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
            try:
                cur = conn.execute(
                    """
                    INSERT INTO properties (
                        guild_id, name, description, image_url, enabled, max_level,
                        buy_price, level_costs_json, level_effects_json,
                        ascension_cost, ascension_effects_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id,
                        name,
                        description,
                        image_url,
                        enabled,
                        max_level,
                        buy_price,
                        json.dumps(normalized_costs, separators=(",", ":")),
                        json.dumps(normalized_effects, separators=(",", ":")),
                        ascension_cost,
                        json.dumps(ascension_effects, separators=(",", ":")),
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                return jsonify({"error": "Property with this name already exists"}), 400
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "id": int(cur.lastrowid), "image_status": image_status, "image_url": image_url})

    @app.post("/api/property/<int:property_id>/update")
    def api_property_update(property_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "name" in data:
            name = str(data.get("name", "")).strip()
            if not name:
                return jsonify({"error": "name cannot be empty"}), 400
            updates["name"] = name
        if "description" in data:
            updates["description"] = str(data.get("description", "")).strip()
        if "image_url" in data:
            updates["image_url"] = str(data.get("image_url", "")).strip()
        if "enabled" in data:
            updates["enabled"] = 1 if int(data.get("enabled", 1) or 1) > 0 else 0
        if "max_level" in data:
            updates["max_level"] = max(1, min(10, int(float(data.get("max_level", 5) or 5))))
        if "buy_price" in data:
            updates["buy_price"] = max(0.0, float(data.get("buy_price", 0.0) or 0.0))
        if "ascension_cost" in data:
            updates["ascension_cost"] = max(0.0, float(data.get("ascension_cost", 0.0) or 0.0))

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            row = conn.execute(
                """
                SELECT max_level, level_costs_json, level_effects_json
                FROM properties
                WHERE guild_id = ? AND id = ?
                """,
                (guild_id, property_id),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Property not found"}), 404
            image_status = "unchanged"
            if "image_url" in updates:
                try:
                    updates["image_url"], image_status = _normalize_image_url_with_status(conn, updates["image_url"])
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400

            max_level = int(updates.get("max_level", row["max_level"]) or 5)
            max_level = max(1, min(10, max_level))
            updates["max_level"] = max_level

            if "level_costs" in data:
                raw_costs = data.get("level_costs")
                if not isinstance(raw_costs, list):
                    return jsonify({"error": "level_costs must be a list"}), 400
                normalized_costs: list[float] = []
                for i in range(max_level):
                    raw = raw_costs[i] if i < len(raw_costs) else 0
                    try:
                        normalized_costs.append(max(0.0, float(raw)))
                    except (TypeError, ValueError):
                        normalized_costs.append(0.0)
                updates["level_costs_json"] = json.dumps(normalized_costs, separators=(",", ":"))
            else:
                try:
                    old_costs = json.loads(str(row["level_costs_json"] or "[]"))
                except json.JSONDecodeError:
                    old_costs = []
                if not isinstance(old_costs, list):
                    old_costs = []
                normalized_costs = []
                for i in range(max_level):
                    raw = old_costs[i] if i < len(old_costs) else 0
                    try:
                        normalized_costs.append(max(0.0, float(raw)))
                    except (TypeError, ValueError):
                        normalized_costs.append(0.0)
                updates["level_costs_json"] = json.dumps(normalized_costs, separators=(",", ":"))

            if "level_effects" in data:
                raw_effects = data.get("level_effects")
                if not isinstance(raw_effects, list):
                    return jsonify({"error": "level_effects must be a list"}), 400
                normalized_effects: list[list[dict]] = []
                for i in range(max_level):
                    raw = raw_effects[i] if i < len(raw_effects) else []
                    normalized_effects.append(_parse_effects_json(raw))
                updates["level_effects_json"] = json.dumps(normalized_effects, separators=(",", ":"))
            else:
                try:
                    old_effects = json.loads(str(row["level_effects_json"] or "[]"))
                except json.JSONDecodeError:
                    old_effects = []
                if not isinstance(old_effects, list):
                    old_effects = []
                normalized_effects = []
                for i in range(max_level):
                    raw = old_effects[i] if i < len(old_effects) else []
                    normalized_effects.append(_parse_effects_json(raw))
                updates["level_effects_json"] = json.dumps(normalized_effects, separators=(",", ":"))

            if "ascension_effects" in data:
                updates["ascension_effects_json"] = json.dumps(
                    _parse_effects_json(data.get("ascension_effects")),
                    separators=(",", ":"),
                )
            updates["updated_at"] = datetime.now(timezone.utc).isoformat()
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            vals = list(updates.values()) + [guild_id, property_id]
            try:
                cur = conn.execute(
                    f"""
                    UPDATE properties
                    SET {set_clause}
                    WHERE guild_id = ? AND id = ?
                    """,
                    vals,
                )
            except sqlite3.IntegrityError:
                return jsonify({"error": "Property with this name already exists"}), 400
            if cur.rowcount <= 0:
                return jsonify({"error": "Property not found"}), 404
            _gc_uploaded_images(conn)
        return jsonify({"ok": True, "image_status": image_status, "image_url": str(updates.get("image_url", ""))})

    @app.post("/api/property/<int:property_id>/delete")
    def api_property_delete(property_id: int):
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            cur = conn.execute(
                """
                DELETE FROM properties
                WHERE guild_id = ? AND id = ?
                """,
                (guild_id, property_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Property not found"}), 404
            _gc_uploaded_images(conn)
        return jsonify({"ok": True})

    @app.post("/api/player/<int:user_id>/update")
    def api_player_update(user_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        bonus_set = False
        bonus_value = 0.0
        if "bank" in data:
            try:
                bank_value = float(data.get("bank"))
                if not math.isfinite(bank_value):
                    raise ValueError("bank must be finite")
                updates["bank"] = max(0.0, bank_value)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for bank"}), 400
        if "networth" in data:
            try:
                networth_value = float(data.get("networth"))
                if not math.isfinite(networth_value):
                    raise ValueError("networth must be finite")
                updates["networth"] = max(0.0, networth_value)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for networth"}), 400
        if "owe" in data:
            try:
                owe_value = float(data.get("owe"))
                if not math.isfinite(owe_value):
                    raise ValueError("owe must be finite")
                updates["owe"] = max(0.0, owe_value)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for owe"}), 400
        if "rank" in data:
            updates["rank"] = str(data.get("rank", "")).strip()
        if "daily_networth_bonus" in data:
            try:
                bonus_value = float(data.get("daily_networth_bonus", 0.0))
                if not math.isfinite(bonus_value):
                    raise ValueError("daily_networth_bonus must be finite")
                bonus_set = True
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for daily_networth_bonus"}), 400
        if not updates and not bonus_set:
            return jsonify({"error": "No valid fields provided"}), 400

        with _connect() as conn:
            row = conn.execute(
                "SELECT guild_id FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(row["guild_id"])
            if updates:
                set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                values = list(updates.values())
                values.append(user_id)
                conn.execute(
                    f"UPDATE users SET {set_clause} WHERE user_id = ?",
                    values,
                )
            if bonus_set:
                _state_set(f"daily_networth_bonus:{guild_id}:{user_id}", str(float(bonus_value)))
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
        if target_stat in DISABLED_PERK_EFFECT_TARGETS:
            return jsonify({"error": "This perk effect target is disabled"}), 400
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
                text_value = str(data.get(text_key, "")).strip().lower()
                if text_key == "target_stat" and text_value in DISABLED_PERK_EFFECT_TARGETS:
                    return jsonify({"error": "This perk effect target is disabled"}), 400
                updates[text_key] = text_value
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
