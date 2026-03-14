"""
Short summary: contains math and statistical utility functions.
"""
import math

Z_SCORES = {
    0.90: 1.645,
    0.95: 1.96,
    0.99: 2.576,
}

def wilson_ci(x: int, n: int, confidence: float = 0.95) -> tuple[float | None, float | None]:
    if n == 0:
        return None, None

    z = Z_SCORES.get(confidence, 1.96)
    p = x / n
    z2 = z * z

    denominator = 1 + z2 / n
    center = (p + z2 / (2 * n)) / denominator
    margin = (z * math.sqrt((p * (1 - p) / n) + (z2 / (4 * n * n)))) / denominator

    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)

    return lower, upper
