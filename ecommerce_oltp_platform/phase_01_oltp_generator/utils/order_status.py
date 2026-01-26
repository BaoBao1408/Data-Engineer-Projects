import random

def weighted_random_status(distribution: dict[str, float]) -> str:
    statuses = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(statuses, weights=weights, k=1)[0]