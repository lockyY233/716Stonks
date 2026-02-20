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
STONKERS_ROLE_INACTIVE = "StonkerBot Sad"
ANNOUNCEMENT_CHANNEL_ID = 0                 # Target channel ID for market close announcements; 0 = auto-pick
ANNOUNCE_MENTION_ROLE = 1                  # 1 = mention stonkers role in close announcement; 0 = no mention
GM_ID = 0                                   # Discord user ID for game master; excluded from ranking leaderboards when >0
ACTIVITY_APPLICATION_ID = 0                 # Discord embedded app (Activity) application ID for /activity launch
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
# Daily close leaderboard bonuses.
# Instant cash bonus paid at close to the top 5 (rank => dollars).
DAILY_CLOSE_TOP_INCOME_BONUS = {
    1: 200.0,
    2: 150.0,
    3: 100.0,
    4: 50.0,
    5: 50.0,
}
# Networth perk-like bonus active until next close for the top 3 (rank => networth add).
DAILY_CLOSE_TOP_NETWORTH_BONUS = {
    1: 50.0,
    2: 30.0,
    3: 15.0,
}

# Rank income paid at market close (once per local date per guild).
RANK_INCOME = {
    "Civilian": 55.0,
    "-Recruit-": 55.0,
    "[PVT] Private": 58.3,
    "[PV2] Private Second Class": 56.1,
    "[PFC] Private First Class": 57.1,
    "[CPL] Corporal": 58.6,
    "[SPC] Specialist": 60.1,
    "[SGT] Sergeant": 62.1,
    "[SSG] Staff Sergeant": 64.1,
    "[SFC] Sergeant First Class": 66.1,
    "[MSG] Master Sergeant": 68.6,
    "[1SG] First Sergeant": 71.1,
    "[SGM] Sergeant Major": 74.0,
    "[CSM] Command Sergeant Major": 77.0,
    "[SMA] Sergeant Major of the Task Force": 81.1,
    "[WO1] Warrant Officer 1": 85.0,
    "[CW2] Chief Warrant Officer 2": 89.1,
    "[CW3] Chief Warrant Officer 3": 93.0,
    "[CW4] Chief Warrant Officer 4": 97.1,
    "[CW5] Chief Warrant Officer 5": 102.1,
    "[2LT] Second Lieutenant": 108.0,
    "[1LT] First Liutenant": 114.0,
    "[CPT] Captain": 121.0,
    "[MAJ] Major": 129.0,
    "[LTC] Lieutenant Colonel": 138.0,
    "[COL] Colonel": 148.0,
    "[BG] Brigadier General": 159.0,
}

DEFAULT_RANK = "Civilian"
