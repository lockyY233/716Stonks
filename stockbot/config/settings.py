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
STONKERS_ROLE_NAME = "📈💰📊Stonkers"        # Role granted on successful registration
DRIFT_NOISE_FREQUENCY = 0.7                 # Normalized [0,1] -> fraction of Nyquist (tick/2)
DRIFT_NOISE_GAIN = 0.8                      # Multiplier on Perlin output so drift% feels closer to direct amplitude
DRIFT_NOISE_LOW_FREQ_RATIO = 0.08           # Low band frequency as a ratio of DRIFT_NOISE_FREQUENCY
DRIFT_NOISE_LOW_GAIN = 3                    # Low band gain as additional multiplier (layered trend-like noise)
TRADING_LIMITS = 40                        # Max shares a player can trade (buy+sell combined) per limit period; <=0 disables
TRADING_LIMITS_PERIOD = 60                  # Reset window in ticks for trading limits; must be >0 when limits enabled
TRADING_FEES = 1                         # Fee as % of realized profit on sell transactions (profit only)
