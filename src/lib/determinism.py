"""Deterministic pseudo-randomness for data generation.

Generation must be byte-reproducible: the idempotency test reruns the whole pipeline and asserts
identical row counts, and the DQ tests assert specific defect counts. ``rand()`` would break both,
because Spark's RNG depends on partitioning and task retries.

Instead every derived value is a pure function of (seed, salt, row id) via ``xxhash64``. Same inputs,
same output, regardless of parallelism, partition count, or retries.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

_RESOLUTION = 1_000_000


def uniform(seed: int, salt: str, key: Column) -> Column:
    """Deterministic uniform double in [0, 1) derived from ``key``."""
    h = F.xxhash64(F.lit(seed), F.lit(salt), key)
    return (F.pmod(h, F.lit(_RESOLUTION)) / F.lit(float(_RESOLUTION))).cast("double")


def bucket(seed: int, salt: str, key: Column, n: int) -> Column:
    """Deterministic integer in [0, n) derived from ``key``."""
    return F.pmod(F.xxhash64(F.lit(seed), F.lit(salt), key), F.lit(n)).cast("int")


def pick(seed: int, salt: str, key: Column, choices: list) -> Column:
    """Deterministically pick one of ``choices`` uniformly."""
    arr = F.array(*[F.lit(c) for c in choices])
    return F.element_at(arr, bucket(seed, salt, key, len(choices)) + F.lit(1))


def weighted_pick(seed: int, salt: str, key: Column, weighted: list[tuple[str, float]]) -> Column:
    """Deterministically pick a value given (value, weight) pairs. Weights need not sum to 1."""
    total = sum(w for _, w in weighted)
    u = uniform(seed, salt, key)
    acc = 0.0
    expr = None
    for value, weight in weighted[:-1]:
        acc += weight / total
        cond = u < F.lit(acc)
        expr = F.when(cond, F.lit(value)) if expr is None else expr.when(cond, F.lit(value))
    # Final bucket catches the remainder, so floating-point drift cannot produce a null.
    return (expr.otherwise(F.lit(weighted[-1][0])) if expr is not None
            else F.lit(weighted[-1][0]))


def chance(seed: int, salt: str, key: Column, probability: float) -> Column:
    """Deterministic boolean true with the given probability."""
    return uniform(seed, salt, key) < F.lit(probability)
