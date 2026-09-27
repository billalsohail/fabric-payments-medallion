# Local entry points. Every target is a one-to-one stand-in for something that would be a Fabric
# item, and the comment on each says which — so the Makefile doubles as a map of the deployment
# target rather than being laptop scaffolding that has to be mentally discarded.
#
#   setup      -> Spark Environment (pinned libraries)
#   seed       -> nb_99_seed_metadata notebook activity
#   generate   -> nb_00 notebook activity (no Fabric equivalent: it stands in for the source systems)
#   run        -> pl_master Data Factory pipeline
#   test       -> the CI gate; nothing about it is Fabric-specific, which is the point
#   dashboard  -> Direct Lake semantic model + Power BI report

VENV    := .venv
PY      := $(VENV)/bin/python
PIP     := $(VENV)/bin/pip
SCALE   ?= tiny
PYTEST_ARGS ?=
# Passed through to orchestration/run.py: --entities, --stages, --force-reload, --until-date,
# --parallelism. Mirrors overriding a pipeline parameter at trigger time on Fabric.
ARGS    ?=

.DEFAULT_GOAL := help
.PHONY: help setup generate seed run idempotency test test-fast lint dashboard clean reset-lake all

help:  ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk -F':.*?## ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# uv rather than pip: the Spark/Delta pairing is version-strict and a lockfile-capable resolver
# makes "it worked on my laptop" reproducible. Python is pinned to 3.11 because PySpark 3.5 does
# not support 3.12+, and the driver and workers must agree on the minor version.
# The stamp depends on pyproject.toml, so a dependency change re-syncs but an ordinary `make run`
# does not pay for a resolver pass. The install is editable because the notebooks import `src` as a
# package — see the [tool.setuptools] comment in pyproject.toml.
$(VENV)/.stamp: pyproject.toml
	@test -x $(PY) || uv venv --python 3.11 $(VENV)
	uv pip install --python $(PY) -e '.[dev]'
	@$(PY) -c "import pyspark, delta; print('pyspark', pyspark.__version__, '/ delta', delta.__version__ if hasattr(delta,'__version__') else '3.2.1')"
	@touch $@

setup: $(VENV)/.stamp  ## Create the venv and install pinned dependencies

generate: setup  ## Generate deterministic landing data (SCALE=tiny|demo)
	$(PY) src/notebooks/nb_00_generate_landing_data.py --scale $(SCALE)

seed: setup  ## Seed the metadata control plane (meta_source_config, meta_dq_rules, ...)
	$(PY) src/notebooks/nb_99_seed_metadata.py

run: setup  ## Run bronze, silver and gold (mirrors the pl_master Fabric pipeline)
	$(PY) -m orchestration.run

# Proves the property the whole design rests on: a rerun of a completed load is a no-op, and a
# forced reload replaces its batch rather than duplicating it. Two distinct mechanisms, so both
# are exercised — see docs/design-decisions.md.
#
# The assertion is over bronze, deliberately: bronze is where a double-load would be irreparable,
# because silver and gold are both rebuilt by MERGE from whatever bronze holds. But all three stages
# run twice here, and gold re-running unconditionally is the point rather than waste — it has no
# watermark, so every run reloads the warehouse from silver and must arrive at the same counts. A
# proc that was not re-runnable from the top would show up here as a row count that moved.
idempotency: setup  ## Run the pipeline twice and assert bronze is unchanged
	$(PY) -m orchestration.run
	$(PY) -m orchestration.run --force-reload
	$(PY) -m pytest tests/test_bronze.py -q -k "rerun or forced_reload or batch_id"

test: setup  ## Full test suite
	$(PY) -m pytest $(PYTEST_ARGS)

test-fast: setup  ## Test suite without the end-to-end/slow cases
	$(PY) -m pytest -m 'not slow' $(PYTEST_ARGS)

# Three checks, cheapest first. The third is a drift check rather than a lint: measures live in the
# TMDL and semantic-model/measures.dax is generated from them, so --check is what keeps the derived
# copy honest. It fails with a diff and the command to fix it — see tools/extract_dax.py.
#
# The T-SQL lint is its own unguarded line, and that is a correction rather than a style choice. It
# used to read `test -d src/warehouse && <linter> || echo "...skipping"`, written while
# src/warehouse did not exist yet. In `sh`, `a && b || c` runs `c` whenever `b` *fails*, not only
# when `a` does — so a genuine lint error took the `echo` branch and the recipe exited 0. `make
# lint` could not fail, and neither could the CI step that runs it. src/warehouse is committed now,
# so the guard has nothing left to guard and the failure path is all it ever affected.
lint: setup  ## Ruff, Fabric T-SQL subset linter, and the measures.dax drift check
	$(VENV)/bin/ruff check src orchestration tools tests dashboard
	$(PY) tools/fabric_tsql_lint.py src/warehouse
	$(PY) tools/extract_dax.py --check

dashboard: setup  ## Build the static dashboard (stand-in for the Direct Lake report)
	$(PY) dashboard/build_dashboard.py

# Deletes the lake but never the landing files: regenerating those costs ~40s and they are the
# stand-in for source systems, which a pipeline is not entitled to delete.
reset-lake:  ## Drop bronze/silver/gold/meta/quarantine, keep landing
	rm -rf _onelake/bronze _onelake/silver _onelake/gold _onelake/meta _onelake/quarantine
	@echo "lake reset — landing preserved. Run 'make seed' next."

clean:  ## Remove the venv, caches and the entire local lake
	rm -rf $(VENV) _onelake .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

all: generate seed run test  ## Cold start: generate, seed, run, test
