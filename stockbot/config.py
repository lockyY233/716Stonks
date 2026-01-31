from pathlib import Path


_ROOT = Path(__file__).resolve().parents[1]
_TOKEN_PATH = _ROOT / "TOKEN"
TOKEN = _TOKEN_PATH.read_text(encoding="utf-8").strip()

ADMIN_IDS = {123456789012345678}  # your Discord user ID(s)
START_BALANCE = 10_000
TICK_SECONDS = 60
