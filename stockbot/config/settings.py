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
MARKET_CLOSE_HOUR = 21.0                    # local close time as decimal hour (e.g. 21.5 = 21:30)
STONKERS_ROLE_NAME = "📈💰📊Stonkers"        # Role granted on successful registration
ANNOUNCEMENT_CHANNEL_ID = 0                 # Target channel ID for market close announcements; 0 = auto-pick
ANNOUNCE_MENTION_ROLE = 1                  # 1 = mention stonkers role in close announcement; 0 = no mention
GM_ID = 0                                   # Discord user ID for game master; excluded from ranking leaderboards when >0
DRIFT_NOISE_FREQUENCY = 0.7                 # Normalized [0,1] -> fraction of Nyquist (tick/2)
DRIFT_NOISE_GAIN = 0.8                      # Multiplier on Perlin output so drift% feels closer to direct amplitude
DRIFT_NOISE_LOW_FREQ_RATIO = 0.08           # Low band frequency as a ratio of DRIFT_NOISE_FREQUENCY
DRIFT_NOISE_LOW_GAIN = 3                    # Low band gain as additional multiplier (layered trend-like noise)
TRADING_LIMITS = 40                        # Max shares a player can trade (buy+sell combined) per limit period; <=0 disables
TRADING_LIMITS_PERIOD = 60                  # Reset window in ticks for trading limits; must be >0 when limits enabled
TRADING_FEES = 1                         # Fee as % of realized profit on sell transactions (profit only)
COMMODITIES_LIMIT = 5                      # Max total commodity units a player can hold across all commodities; <=0 disables
PAWN_SELL_RATE = 75                        # Pawn payout rate (% of commodity price when selling to bank)
SHOP_RARITY_WEIGHTS = '{"common":1.0,"uncommon":0.6,"rare":0.3,"legendary":0.1,"exotic":0.03}'  # Base rarity spawn weights for /shop rotation
OWNER_BUY_FEE_RATE = 5                     # Company owner earns this % from non-owner buy transaction value

# Rank income paid at market close (once per local date per guild).
RANK_INCOME = {
    "Civilian": 5.0,
    "-Recruit-": 5.0,
    "[PVT] Private": 11.5,
    "[PV2] Private Second Class": 7.1,
    "[PFC] Private First Class": 9.1,
    "[CPL] Corporal": 12.0,
    "[SPC] Specialist": 15.0,
    "[SGT] Sergeant": 18.9,
    "[SSG] Staff Sergeant": 22.8,
    "[SFC] Sergeant First Class": 26.7,
    "[MSG] Master Sergeant": 31.6,
    "[1SG] First Sergeant": 36.5,
    "[SGM] Sergeant Major": 42.3,
    "[CSM] Command Sergeant Major": 48.2,
    "[SMA] Sergeant Major of the Task Force": 56.1,
    "[WO1] Warrant Officer 1": 63.9,
    "[CW2] Chief Warrant Officer 2": 71.8,
    "[CW3] Chief Warrant Officer 3": 79.6,
    "[CW4] Chief Warrant Officer 4": 87.5,
    "[CW5] Chief Warrant Officer 5": 97.3,
    "[2LT] Second Lieutenant": 109.0,
    "[1LT] First Liutenant": 120.8,
    "[CPT] Captain": 134.5,
    "[MAJ] Major": 150.2,
    "[LTC] Lieutenant Colonel": 167.8,
    "[COL] Colonel": 187.4,
    "[BG] Brigadier General": 209.0,
}

DEFAULT_RANK = "Civilian"
