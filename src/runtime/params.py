"""Notebook parameter resolution, mirroring Fabric's parameter-cell mechanism.

On Fabric, a pipeline Notebook activity injects parameters by overwriting a cell tagged
``parameters``. Locally there is no such mechanism, so parameters arrive as CLI flags or
environment variables. Notebooks declare what they accept by calling :func:`resolve` with defaults,
which keeps the declaration in one visible place — the same role the parameter cell plays on Fabric.

Precedence: CLI flag > environment variable > default.
"""

from __future__ import annotations

import argparse
import os
from typing import Any


def _coerce(value: str, default: Any) -> Any:
    if isinstance(default, bool):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    if isinstance(default, int) and not isinstance(default, bool):
        return int(value)
    if isinstance(default, float):
        return float(value)
    return value


def resolve(defaults: dict[str, Any], argv: list[str] | None = None) -> dict[str, Any]:
    """Resolve declared parameters from CLI flags, then env vars, then defaults.

    Environment variable name for parameter ``scale`` is ``PARAM_SCALE``.
    """
    parser = argparse.ArgumentParser(add_help=True)
    for key, default in defaults.items():
        parser.add_argument(f"--{key.replace('_', '-')}", dest=key, default=None)
    # parse_known_args so a notebook can be invoked by a driver passing unrelated flags.
    args, _unknown = parser.parse_known_args(argv)

    resolved: dict[str, Any] = {}
    for key, default in defaults.items():
        cli = getattr(args, key, None)
        if cli is not None:
            resolved[key] = _coerce(str(cli), default)
            continue
        env = os.environ.get(f"PARAM_{key.upper()}")
        if env is not None and env != "":
            resolved[key] = _coerce(env, default)
            continue
        resolved[key] = default
    return resolved
