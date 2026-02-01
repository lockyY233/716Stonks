from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2]
_TOKEN_PATH = _ROOT / "TOKEN"
TOKEN = _TOKEN_PATH.read_text(encoding="utf-8").strip()
DB_PATH = _ROOT / "data" / "stockbot.db"

# APP CONFIGS
ADMIN_IDS = {123456789012345678}            # your Admin Discord user ID(s)
START_BALANCE = 5                           # Players starting balance
TICK_INTERVAL = 5                           # 1 tick = 1 update to the pricing
TREND_MULTIPLIER = 1e-2                     # Multiplier to the slope 
DISPLAY_TIMEZONE = "America/New_York"       # Timezone for display only
MARKET_CLOSE_HOUR = 21                      # hour to record daily close (reset changes and send tag players for news)
