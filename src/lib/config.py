"""Reading the control plane.

`meta_source_config` is read by both notebooks and by the orchestrator, so the reader lives here
rather than in whichever notebook happened to need it first. That is not tidiness: on Fabric a
notebook cannot `import` another notebook, only `%run` it, and a `%run` executes the whole thing.
A helper two notebooks share has to be library code attached to the Spark Environment, or the
"unmodified Fabric notebook code" claim in the README stops being true.

Two conversions happen here and nowhere else, so every caller sees the same shapes:

- **`read_options`** is stored as a JSON string and handed back as a dict.
- **`merge_keys`** is stored comma-separated and handed back as a list.

Both are stored as strings because a Data Factory `Lookup` activity hands pipeline expressions
strings — a config schema that only works from Spark is not configuration, it is a second codebase.
Parsing at the point of use is the price of that, and it is cheap.
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from src.runtime.context import Layer, read_table

TABLE = "meta_source_config"


def load_source_config(entity: str) -> dict:
    """The config row for one entity, with its string-encoded fields parsed.

    A missing row is fatal and says what *is* configured. The alternative — defaulting — would mean
    a typo in an entity name produces a run that reads nothing and reports success.
    """
    if not entity:
        raise ValueError("parameter `entity` is required")
    rows = read_table(Layer.META, TABLE).filter(F.col("entity") == entity).collect()
    if not rows:
        available = [r["entity"] for r in read_table(Layer.META, TABLE).select("entity").collect()]
        raise ValueError(
            f"no {TABLE} row for entity {entity!r}. Configured: {sorted(available)}. "
            "Run nb_99_seed_metadata to seed the control plane."
        )
    cfg = rows[0].asDict()
    cfg["read_options"] = json.loads(cfg["read_options"]) if cfg["read_options"] else {}
    cfg["merge_keys"] = [k.strip() for k in (cfg["merge_keys"] or "").split(",") if k.strip()]
    return cfg


def require_enabled(cfg: dict) -> None:
    """Refuse to process a feed whose config row is disabled.

    Bypassing the flag would give the pipeline two sources of truth about which feeds are live, and
    the one in the config table would be the one people read.
    """
    if not cfg["enabled"]:
        raise ValueError(
            f"{cfg['entity']} is disabled in {TABLE}. Enable it there rather than bypassing "
            "config — a feed loaded despite its config row is a feed nobody can reason about."
        )
