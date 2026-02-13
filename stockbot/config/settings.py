from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2]
_TOKEN_PATH = _ROOT / "TOKEN"
TOKEN = _TOKEN_PATH.read_text(encoding="utf-8").strip()
DB_PATH = _ROOT / "data" / "stockbot.db"

# APP CONFIGS
START_BALANCE = 5                           # Players starting balance
TICK_INTERVAL = 5                           # 1 tick = 1 update to the pricing
TREND_MULTIPLIER = 1e-2                     # Multiplier to the slope 
DISPLAY_TIMEZONE = "America/New_York"       # Timezone for display only
MARKET_CLOSE_HOUR = 21                      # hour to record daily close (reset changes and send tag players for news)
STONKERS_ROLE_NAME = "📈💰📊Stonkers"        # Role granted on successful registration
ANNOUNCEMENT_CHANNEL_ID = 0                 # Target channel ID for market close announcements; 0 = auto-pick
DRIFT_NOISE_FREQUENCY = 0.7                 # Normalized [0,1] -> fraction of Nyquist (tick/2)
DRIFT_NOISE_GAIN = 0.8                      # Multiplier on Perlin output so drift% feels closer to direct amplitude
DRIFT_NOISE_LOW_FREQ_RATIO = 0.08           # Low band frequency as a ratio of DRIFT_NOISE_FREQUENCY
DRIFT_NOISE_LOW_GAIN = 3                    # Low band gain as additional multiplier (layered trend-like noise)
TRADING_LIMITS = 40                        # Max shares a player can trade (buy+sell combined) per limit period; <=0 disables
TRADING_LIMITS_PERIOD = 60                  # Reset window in ticks for trading limits; must be >0 when limits enabled
TRADING_FEES = 1                         # Fee as % of realized profit on sell transactions (profit only)

# Rank income paid at market close (once per local date per guild).
RANK_INCOME = {
    "Civilian": 5.0,
    "-Recruit-": 5.0,
    "[PVT] Private": 11.5,
    "[PV2] Private Second Class": 15.8,
    "[PFC] Private First Class": 20.2,
    "[CPL] Corporal": 26.7,
    "[SPC] Specialist": 33.3,
    "[SGT] Sergeant": 42.0,
    "[SSG] Staff Sergeant": 50.7,
    "[SFC] Sergeant First Class": 59.4,
    "[MSG] Master Sergeant": 70.3,
    "[1SG] First Sergeant": 81.2,
    "[SGM] Sergeant Major": 94.2,
    "[CSM] Command Sergeant Major": 107.3,
    "[SMA] Sergeant Major of the Task Force": 124.8,
    "[WO1] Warrant Officer 1": 142.2,
    "[CW2] Chief Warrant Officer 2": 159.7,
    "[CW3] Chief Warrant Officer 3": 177.1,
    "[CW4] Chief Warrant Officer 4": 194.6,
    "[CW5] Chief Warrant Officer 5": 216.4,
    "[2LT] Second Lieutenant": 242.5,
    "[1LT] First Liutenant": 268.7,
    "[CPT] Captain": 299.2,
    "[MAJ] Major": 334.1,
    "[LTC] Lieutenant Colonel": 373.3,
    "[COL] Colonel": 416.9,
    "[BG] Brigadier General": 465.0,
}

DEFAULT_RANK = "Civilian"
