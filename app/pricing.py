from __future__ import annotations

from statistics import median
from typing import Iterable

SCENARIO_FACTORS = {
    'research': 0.95,
    'analytics': 1.00,
    'commercial': 1.12,
    'decision': 1.18,
}


def quality_score(metrics: dict[str, float]) -> float:
    weights = {
        'completeness': 0.25,
        'accuracy': 0.25,
        'timeliness': 0.20,
        'consistency': 0.15,
        'availability': 0.15,
    }
    score = 0.0
    for key, weight in weights.items():
        score += float(metrics.get(key, 0.7)) * weight
    return round(min(max(score, 0.0), 1.0), 4)


def filter_anomalies(ratings: Iterable[float]) -> list[float]:
    values = [float(x) for x in ratings]
    if len(values) <= 3:
        return values
    med = median(values)
    deviations = [abs(v - med) for v in values]
    mad = median(deviations) or 1e-6
    filtered = [v for v in values if abs(v - med) / mad <= 3.5]
    return filtered or values


def trust_score(reputation: float, ratings: Iterable[float], success_rate: float = 1.0) -> float:
    filtered = filter_anomalies(ratings)
    avg_rating = (sum(filtered) / len(filtered) / 5.0) if filtered else 0.75
    score = 0.50 * reputation + 0.30 * avg_rating + 0.20 * success_rate
    return round(min(max(score, 0.0), 1.1), 4)


def boundary_factor(duration_days: int, download_limit: int, scope_factor: float) -> float:
    dur = 0.90 + min(duration_days, 365) / 365.0 * 0.20
    dl = 0.90 + min(download_limit, 50) / 50.0 * 0.15
    sf = max(0.8, min(scope_factor, 1.5))
    return round(dur * dl * sf, 4)


def compute_price(
    *,
    base_price: float,
    quality_metrics: dict[str, float],
    reputation: float,
    ratings: Iterable[float],
    success_rate: float,
    scenario: str,
    duration_days: int,
    download_limit: int,
    scope_factor: float,
) -> dict[str, float]:
    q_score = quality_score(quality_metrics)
    t_score = trust_score(reputation, ratings, success_rate)
    s_factor = SCENARIO_FACTORS.get(scenario, 1.0)
    b_factor = boundary_factor(duration_days, download_limit, scope_factor)
    price = base_price * (0.55 + q_score) * (0.60 + t_score) * s_factor * b_factor
    return {
        'quality_score': q_score,
        'trust_score': t_score,
        'scenario_factor': round(s_factor, 4),
        'boundary_factor': b_factor,
        'price': round(price, 2),
    }
