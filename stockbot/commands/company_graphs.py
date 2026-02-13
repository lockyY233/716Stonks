from io import BytesIO

import matplotlib
from discord import File
from matplotlib import pyplot as plt

from stockbot.config.runtime import get_app_config
from stockbot.core.noise import (
    normalized_frequency_to_cycles_per_tick,
    perlin1d,
    stable_seed,
)

matplotlib.use("Agg")


def build_player_impact_curve(
    *,
    symbol: str,
    slope: float,
    drift: float,
    liquidity: float,
    impact_power: float,
) -> File:
    # Base delta per tick under simplified model:
    # delta_base = slope + sign((buy-sell)/liquidity) * abs((buy-sell)/liquidity)^impact_power
    span = 100
    x_values = list(range(-span, span + 1))
    y_values = []
    for x in x_values:
        flow = x / liquidity
        nonlinear = (abs(flow) ** impact_power) * (1.0 if flow >= 0 else -1.0)
        y_values.append(slope + nonlinear)

    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    ax.axhline(0, color="#666666", linewidth=1.0, alpha=0.7)
    ax.axvline(0, color="#666666", linewidth=1.0, alpha=0.7)
    ax.plot(x_values, y_values, color="#2ecc71", linewidth=2.5)
    ax.set_ylim(-1.0, 1.0)
    ax.set_title(f"{symbol} Base Delta Per Tick")
    ax.set_xlabel("Net Shares In Tick (buy - sell)")
    ax.set_ylabel("Delta Base Price")
    ax.grid(True, alpha=0.2)
    fig.text(
        0.02,
        0.01,
        f"slope={slope:.4f}  drift=±{drift:.3f}%  liquidity={liquidity:.2f}  impact_power={impact_power:.3f}",
        transform=fig.transFigure,
        va="bottom",
        ha="left",
        fontsize=9,
        color="#444444",
        bbox={"boxstyle": "round,pad=0.25", "facecolor": "#ffffffcc", "edgecolor": "#cccccc"},
    )

    buf = BytesIO()
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    buf.seek(0)
    return File(buf, filename=f"{symbol.lower()}_impact_curve.png")

def build_price_projection_curve(
    *,
    symbol: str,
    start_price: float,
    slope: float,
    drift: float,
    liquidity: float,
    impact_power: float,
    ticks: int = 80,
) -> File:
    # Simulate no-trade future path from current settings with 1D Perlin noise.
    drift_noise_frequency = float(get_app_config("DRIFT_NOISE_FREQUENCY"))
    drift_noise_low_freq_ratio = float(get_app_config("DRIFT_NOISE_LOW_FREQ_RATIO"))
    drift_noise_gain = float(get_app_config("DRIFT_NOISE_GAIN"))
    drift_noise_low_gain = float(get_app_config("DRIFT_NOISE_LOW_GAIN"))
    drift_ratio = abs(float(drift)) / 100.0
    fast_cycles_per_tick = normalized_frequency_to_cycles_per_tick(drift_noise_frequency)
    low_cycles_per_tick = fast_cycles_per_tick * max(0.0, drift_noise_low_freq_ratio)
    fast_gain = max(0.0, drift_noise_gain)
    low_gain = max(0.0, drift_noise_low_gain)
    seed_fast = stable_seed(symbol.upper())
    seed_low = stable_seed(f"{symbol.upper()}:low")
    phase_fast = (seed_fast % 10007) / 10007.0
    phase_low = (seed_low % 10007) / 10007.0
    base = max(0.01, float(start_price))
    liquidity = max(1.0, float(liquidity))
    impact_power = max(0.1, float(impact_power))
    x_values = list(range(ticks + 1))
    base_values = [base]
    sampled_values = [base]
    buy_tick = 20
    sell_tick = 60
    scripted_trade_size = 10.0

    for step in range(1, ticks + 1):
        scripted_flow = 0.0
        if step == buy_tick:
            scripted_flow = scripted_trade_size / liquidity
        elif step == sell_tick:
            scripted_flow = -scripted_trade_size / liquidity
        scripted_impact = (abs(scripted_flow) ** impact_power) * (1.0 if scripted_flow >= 0 else -1.0)
        base = max(0.01, base + float(slope) + scripted_impact)
        x_fast = (step * fast_cycles_per_tick) + phase_fast
        x_low = (step * low_cycles_per_tick) + phase_low
        noise_unit = (perlin1d(x_fast, seed_fast) * fast_gain) + (perlin1d(x_low, seed_low) * low_gain)
        noise = noise_unit * drift_ratio * base
        sampled = max(0.01, base + noise)
        base_values.append(base)
        sampled_values.append(sampled)

    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    ax.plot(x_values, base_values, color="#3498db", linewidth=2.2, label="Base path (no noise)")
    ax.plot(x_values, sampled_values, color="#f39c12", linewidth=2.0, alpha=0.95, label="Simulated price")
    ax.axvline(buy_tick, color="#2ecc71", linewidth=1.4, linestyle="--", alpha=0.8, label="Buy 10 @ t=20")
    ax.axvline(sell_tick, color="#e74c3c", linewidth=1.4, linestyle="--", alpha=0.8, label="Sell 10 @ t=60")
    y_min = max(0.01, float(start_price) - 5.0)
    y_max = max(y_min + 0.01, float(start_price) + 5.0)
    ax.set_ylim(y_min, y_max)
    ax.set_title(f"{symbol} Price Simulation ({ticks} ticks)")
    ax.set_xlabel("Ticks")
    ax.set_ylabel("Price")
    ax.grid(True, alpha=0.2)
    ax.legend(loc="upper left")
    fig.text(
        0.02,
        0.01,
        f"start={start_price:.2f}  slope={slope:.4f}/tick  drift=±{drift:.3f}%/tick  f={drift_noise_frequency:.2f}  g={drift_noise_gain:.2f}  low={drift_noise_low_freq_ratio:.2f}x/{drift_noise_low_gain:.2f}  events:+10@20,-10@60",
        transform=fig.transFigure,
        va="bottom",
        ha="left",
        fontsize=9,
        color="#444444",
        bbox={"boxstyle": "round,pad=0.25", "facecolor": "#ffffffcc", "edgecolor": "#cccccc"},
    )

    buf = BytesIO()
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    buf.seek(0)
    return File(buf, filename=f"{symbol.lower()}_projection_curve.png")
