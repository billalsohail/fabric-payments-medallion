"""Notebook parameter resolution, mirroring Fabric's parameter-cell mechanism.

On Fabric, a pipeline Notebook activity injects parameters by overwriting a cell tagged
``parameters``. Locally there is no such mechanism, so parameters arrive as CLI flags or
environment variables. Notebooks declare what they accept by calling :func:`resolve` with defaults,
which keeps the declaration in one visible place — the same role the parameter cell plays on Fabric.

Precedence: CLI flag > environment variable > default.

**CLI flags apply only to the notebook being run directly.** A notebook's parameter cell executes on
import, so when `orchestration/run.py` imports `nb_01_bronze_ingest` to call it as a library, a naive
`parse_known_args(sys.argv)` would parse the *orchestrator's* flags into the notebook's parameters —
and silently, since unknown flags are tolerated. `--force-reload` belonging to both is enough to
break it. `resolve` therefore checks whether its caller is `__main__` and ignores argv when it is
not, which matches the Fabric semantics exactly: the parameter cell is overwritten for the notebook
the pipeline is invoking, and an imported helper takes its values from the environment or defaults.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any


def _coerce(value: str, default: Any) -> Any:
    if isinstance(default, bool):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    if isinstance(default, int) and not isinstance(default, bool):
        return int(value)
    if isinstance(default, float):
        return float(value)
    return value


def _caller_is_main(depth: int = 2) -> bool:
    """True when the module calling :func:`resolve` is the one being executed."""
    try:
        return sys._getframe(depth).f_globals.get("__name__") == "__main__"
    except ValueError:  # pragma: no cover — frame depth unavailable
        return False


def resolve(defaults: dict[str, Any], argv: list[str] | None = None) -> dict[str, Any]:
    """Resolve declared parameters from CLI flags, then env vars, then defaults.

    Environment variable name for parameter ``scale`` is ``PARAM_SCALE``. Pass ``argv`` explicitly to
    override the "only when run directly" rule — tests do this to exercise flag parsing.
    """
    if argv is None and not _caller_is_main():
        argv = []
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
