from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Ensure project root is importable when running as a standalone script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stockbot.config.runtime import get_app_config


DISCORD_API_BASE = "https://discord.com/api/v10"


def _read_default_token() -> str:
    token_path = Path(__file__).resolve().parents[1] / "TOKEN"
    if token_path.exists():
        return token_path.read_text(encoding="utf-8").strip()
    return ""


def _request_json(method: str, url: str, token: str, payload: dict | None = None) -> object:
    data = None
    headers = {
        "Authorization": f"Bot {token}",
        "User-Agent": "716Stonks/entrypoint-setup",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url=url, method=method, headers=headers, data=data)
    with urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _find_existing_primary_entrypoint(commands: list[dict]) -> dict | None:
    for cmd in commands:
        if int(cmd.get("type", 0)) == 4:
            return cmd
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Register or update Discord PRIMARY_ENTRY_POINT activity command.",
    )
    parser.add_argument(
        "--application-id",
        default="",
        help="Discord application ID. Default: ACTIVITY_APPLICATION_ID app config.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("DISCORD_BOT_TOKEN", "").strip() or _read_default_token(),
        help="Bot token. Default: DISCORD_BOT_TOKEN env or TOKEN file.",
    )
    parser.add_argument(
        "--name",
        default="Launch 716Stonks",
        help="Display name for primary entry point command.",
    )
    parser.add_argument(
        "--description",
        default="Launch 716Stonks Activity",
        help="Description for primary entry point command.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload only, do not call Discord API.",
    )
    args = parser.parse_args()

    token = str(args.token or "").strip()
    if not token:
        print("Missing bot token. Set --token or DISCORD_BOT_TOKEN, or provide TOKEN file.")
        return 2

    app_id_raw = str(args.application_id or "").strip()
    if not app_id_raw:
        app_id_raw = str(get_app_config("ACTIVITY_APPLICATION_ID")).strip()
    if not app_id_raw.isdigit():
        print("Missing/invalid application ID. Set --application-id or ACTIVITY_APPLICATION_ID app config.")
        return 2
    app_id = app_id_raw

    payload = {
        "name": str(args.name).strip() or "Launch 716Stonks",
        "description": str(args.description).strip() or "Launch 716Stonks Activity",
        "type": 4,      # PRIMARY_ENTRY_POINT
        "handler": 2,   # DISCORD_LAUNCH_ACTIVITY
    }

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    base_url = f"{DISCORD_API_BASE}/applications/{app_id}/commands"
    try:
        current = _request_json("GET", base_url, token)
        if not isinstance(current, list):
            print("Unexpected GET /commands response:", current)
            return 1
        existing = _find_existing_primary_entrypoint(current)

        if existing is None:
            created = _request_json("POST", base_url, token, payload)
            print("Created PRIMARY_ENTRY_POINT command:")
            print(json.dumps(created, indent=2))
            return 0

        cmd_id = str(existing.get("id", "")).strip()
        if not cmd_id:
            print("Found existing PRIMARY_ENTRY_POINT but missing id.")
            return 1
        updated = _request_json("PATCH", f"{base_url}/{cmd_id}", token, payload)
        print("Updated PRIMARY_ENTRY_POINT command:")
        print(json.dumps(updated, indent=2))
        return 0
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            body = "<unable to read response body>"
        print(f"Discord API error: {exc.code} {exc.reason}\n{body}")
        return 1
    except URLError as exc:
        print(f"Network error: {exc}")
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
