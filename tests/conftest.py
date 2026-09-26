"""Shared fixtures.

The interesting one is `isolated_lake`. Bronze tests have to *write*, and writing into the repo's
`_onelake/` would make the suite order-dependent and leave the demo dataset in whatever state the
last test left it. So each test module gets a fresh lake root with `files/` **symlinked** to the
real landing data and every other layer empty.

Symlinking rather than copying is the point: regenerating landing data costs ~40s and bronze tests
do not mutate it, so they share one read-only copy while getting private bronze, silver and meta
layers. The alternative — one shared root plus careful cleanup — is the kind of thing that passes
locally and fails in CI on a different test order.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.runtime import context

REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_LANDING = REPO_ROOT / "_onelake" / "files"


def point_at(root: Path) -> Path:
    """Repoint the runtime at `root`, creating the landing symlink if needed."""
    root.mkdir(parents=True, exist_ok=True)
    link = root / "files"
    if not link.exists():
        link.symlink_to(REAL_LANDING)
    os.environ["ONELAKE_ROOT"] = str(root)
    context.reset_caches()
    return root


@pytest.fixture(scope="session", autouse=True)
def _require_landing_data():
    if not (REAL_LANDING / "landing" / "transactions").is_dir():
        pytest.skip("no landing data — run `make seed-data` (nb_00) first", allow_module_level=True)


@pytest.fixture(scope="module")
def isolated_lake(tmp_path_factory):
    """A private lake root with the control plane seeded and landing data linked in."""
    from src.notebooks import nb_99_seed_metadata as nb99

    root = point_at(tmp_path_factory.mktemp("lake"))
    nb99.main()
    return root


@pytest.fixture(autouse=True)
def _restore_lake(request):
    """Re-point at the module's lake before every test.

    A test that deliberately builds its own root (the schema-evolution case needs one it can load in
    two passes) therefore cannot leak that root into its neighbours.
    """
    lake = request.node.get_closest_marker("uses_isolated_lake")
    yield
    if lake is None and "isolated_lake" in getattr(request, "fixturenames", ()):
        point_at(Path(request.getfixturevalue("isolated_lake")))
