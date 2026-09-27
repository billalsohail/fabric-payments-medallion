"""Deploy `fabric/build/` into a Fabric workspace with `fabric-cicd` — UNVALIDATED.

**This script has never been run.** Not once, against anything. It is the file
`docs/fabric-deployment.md` §7 promises, written so that the first person with a tenant executes a
reviewed script instead of improvising, and every claim in it about `fabric-cicd`'s behaviour comes
from its documentation ([microsoft.github.io/fabric-cicd](https://microsoft.github.io/fabric-cicd/))
rather than from having watched it work. Treat a green run as new information.

**Three properties of `fabric-cicd` that the interface below exists to make hard to get wrong.**

1. **It deploys into the tenant of the executing identity.** There is no tenant argument. Whoever is
   logged in to `az` is who this runs as, and `--workspace-id` is therefore required and never
   defaulted — a deploy script that guesses a workspace is a deploy script that eventually
   overwrites the wrong one.
2. **It publishes everything, every time.** `publish_all_items()` does not consult commit history or
   diff against what is deployed; it uploads the definition of every item it finds. That makes a
   deploy idempotent and makes the repository, not the workspace, the thing that is true — the same
   bargain `nb_01`'s `_batch_id` delete-then-insert strikes with bronze.
3. **`unpublish_all_orphan_items()` deletes.** It removes workspace items absent from the repository,
   which is how a renamed item stops existing twice, and is also how a mis-scoped run empties a
   workspace. It is behind `--unpublish-orphans` and defaults off.

**What this script does not deploy, and that is deliberate.** `wh_gold` is absent from
`fabric/items/` entirely. Fabric git integration represents a warehouse as a SQL database project
extracted by DacFx, so `src/warehouse/ddl/*.sql` and `src/warehouse/procs/*.sql` cannot be dropped
into a `wh_gold.Warehouse/` directory and synced — §4 explains the choice at length. The repo's
scripts stay authoritative and the warehouse is deployed by **executing** them, which makes it the
one item in the inventory whose deployment is imperative. This script refuses to pretend otherwise:
it prints that step rather than performing it.

**There is no `parameter.yml`, and its absence is the design.** `fabric-cicd` parameterises by
find-and-replace inside item definitions, keyed by `environment`, and the canonical thing people
replace is a notebook's default-lakehouse GUID. `fabric/build_items.py` binds no default lakehouse —
`src/runtime/context.py` names tables two-part so that no notebook depends on which lakehouse is
default — and every value that genuinely differs by stage lives in `vl_payments.VariableLibrary`
instead. Nothing is left for a parameter file to substitute, so there is no parameter file. If a
future item needs one, that is the moment to add it, not before.

Usage:

    az login
    python fabric/build_items.py
    python fabric/deploy.py --workspace-id <guid> --environment dev
    python fabric/deploy.py --workspace-id <guid> --environment dev --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "fabric" / "build"

# `--environment` selects a `parameter.yml` section for fabric-cicd and, here, names the Variable
# Library value set a human then activates. The active value set is item *configuration* rather than
# *definition*, so no deploy sets it and no deploy overwrites it — §6 says so, and it is the reason
# this script cannot finish the job by itself.
ENVIRONMENTS = ("dev", "test", "prod")

WAREHOUSE_STEP = """
wh_gold is not in this deployment and was not touched. It is imperative: create the Warehouse item,
then execute in order

    src/warehouse/ddl/01_schemas.sql ... 05_security.sql
    src/warehouse/procs/*.sql

against its T-SQL endpoint. `tools/fabric_tsql_lint.py` has already proved those scripts are inside
the Fabric Warehouse surface area; what it cannot prove is how they plan. See docs/fabric-deployment
section 1 for the two questions to answer while you are connected.
""".strip()

VALUE_SET_STEP = """
The Variable Library's active value set is configuration, not definition, so this deploy did not set
it. Open vl_payments and activate the value set for this stage. `prod` sets Scale to the empty
string, which nb_00_generate_landing_data raises on by design: in prod the data generator is meant to
be unable to run, not merely discouraged from running.
""".strip()


def _fabric_workspace(workspace_id: str, environment: str):
    """Build the `FabricWorkspace`, importing lazily so `--dry-run` needs no credentials.

    `fabric-cicd` and `azure-identity` are deliberately not dependencies of this project: they are
    needed by a script that cannot run here, and installing them would put two Azure SDK trees into
    the environment that `make test` builds in CI for no test's benefit.
    """
    try:
        from azure.identity import AzureCliCredential
        from fabric_cicd import FabricWorkspace
    except ImportError as exc:  # pragma: no cover - requires the tenant-side extras
        raise SystemExit(
            f"{exc.name} is not installed. It is not a dependency of this repo on purpose; see the "
            f"_fabric_workspace docstring. Install with:\n"
            f"    uv pip install fabric-cicd azure-identity"
        ) from exc

    # Every FabricWorkspace argument must be a keyword argument — the library rejects positional
    # ones, which is a deliberate guard against transposing two GUID-shaped strings.
    return FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory=str(BUILD),
        token_credential=AzureCliCredential(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace-id", required=True, help="target workspace GUID; never defaulted")
    parser.add_argument("--environment", required=True, choices=ENVIRONMENTS)
    parser.add_argument(
        "--unpublish-orphans",
        action="store_true",
        help="also DELETE workspace items absent from fabric/build/ (off by default)",
    )
    parser.add_argument("--dry-run", action="store_true", help="report what would be sent, send nothing")
    args = parser.parse_args()

    if not BUILD.is_dir():
        raise SystemExit(
            f"{BUILD.relative_to(ROOT)}/ does not exist. It is generated and gitignored — run "
            f"`make fabric-build` first."
        )
    items = sorted(p.name for p in BUILD.iterdir() if (p / ".platform").is_file())
    if not items:
        raise SystemExit(f"{BUILD.relative_to(ROOT)}/ holds no items with a .platform; rebuild it")

    print(f"workspace   {args.workspace_id}")
    print(f"environment {args.environment}")
    print(f"source      {BUILD.relative_to(ROOT)}/ ({len(items)} items)")
    for name in items:
        print(f"    {name}")
    print(f"orphans     {'DELETE' if args.unpublish_orphans else 'left alone'}")

    if args.dry_run:
        print("\n--dry-run: nothing sent. Neither the workspace nor your credentials were touched.")
        print(f"\n{WAREHOUSE_STEP}\n\n{VALUE_SET_STEP}")
        return

    print(
        "\nDeploying as the identity `az` is logged in as, into that identity's tenant.\n"
        "Every item above is published in full; commit history is not consulted.",
        flush=True,
    )
    workspace = _fabric_workspace(args.workspace_id, args.environment)
    from fabric_cicd import publish_all_items, unpublish_all_orphan_items

    publish_all_items(workspace)
    if args.unpublish_orphans:
        unpublish_all_orphan_items(workspace)

    print(f"\n{WAREHOUSE_STEP}\n\n{VALUE_SET_STEP}")
    print(
        "\nThis is the first time this script has run. Nothing above was verified before today — "
        "if it worked, docs/fabric-deployment.md §9 has a shorter list than it did this morning.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
