from math import floor


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def normalized_frequency_to_cycles_per_tick(freq_norm: float) -> float:
    # Normalized frequency in [0, 1] mapped to Nyquist fraction:
    # 0 -> 0 cycles/tick, 1 -> 0.5 cycles/tick (Nyquist).
    return 0.5 * clamp01(freq_norm)


def stable_seed(text: str) -> int:
    h = 2166136261
    for ch in text:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def _hash_u32(i: int, seed: int) -> int:
    x = (i ^ seed) & 0xFFFFFFFF
    x ^= (x >> 16)
    x = (x * 0x7FEB352D) & 0xFFFFFFFF
    x ^= (x >> 15)
    x = (x * 0x846CA68B) & 0xFFFFFFFF
    x ^= (x >> 16)
    return x


def _grad(i: int, seed: int) -> float:
    # Gradient in [-1, 1].
    h = _hash_u32(i, seed)
    return (h / 2147483647.5) - 1.0


def _fade(t: float) -> float:
    # Quintic smoothstep.
    return t * t * t * (t * (t * 6.0 - 15.0) + 10.0)


def perlin1d(x: float, seed: int) -> float:
    x0 = floor(x)
    x1 = x0 + 1
    t = x - x0

    g0 = _grad(x0, seed)
    g1 = _grad(x1, seed)

    n0 = g0 * (x - x0)
    n1 = g1 * (x - x1)
    u = _fade(t)
    value = n0 + (n1 - n0) * u

    # Scale to near [-1, 1] for convenient drift amplitude mapping.
    return max(-1.0, min(1.0, value * 2.0))
