from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run sqlite_web for stockbot DB.")
    parser.add_argument(
        "--host",
        default=os.getenv("SQLWEB_HOST", "0.0.0.0"),
        help="Host bind address (default: 0.0.0.0 or SQLWEB_HOST).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("SQLWEB_PORT", "8081")),
        help="Port (default: 8081 or SQLWEB_PORT).",
    )
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=int(os.getenv("SQLWEB_MAX_SECONDS", "0")),
        help="Auto-stop after N seconds (0 = run until stopped).",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("SQLWEB_DB_PATH", ""),
        help="Optional DB path override. Defaults to repo data/stockbot.db.",
    )
    return parser.parse_args()


def main() -> None:
    args = _build_args()
    repo_root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db_path).expanduser() if args.db_path else (repo_root / "data" / "stockbot.db")
    cmd = [
        sys.executable,
        "-m",
        "sqlite_web",
        str(db_path),
        "--host",
        str(args.host),
        "--port",
        str(args.port),
    ]
    proc = subprocess.Popen(cmd)
    max_seconds = max(0, int(args.max_seconds))
    if max_seconds <= 0:
        raise SystemExit(proc.wait())
    try:
        proc.wait(timeout=max_seconds)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    main()
