"""Every path this repository cites is a path this repository has.

The documents here cross-reference constantly, and that habit is most of what makes them navigable:
`docs/architecture.md` points at the seams, `README.md` §9 is a map of the tree, and half the module
docstrings name the page whose claim they implement. A citation that has gone stale is worse than no
citation, because the reader spends their attention finding out it is wrong rather than reading the
thing it promised.

This is the last test written, and it was written last on purpose. For most of this project's life
`docs/fabric-deployment.md` §7 named `fabric/deploy.py` — a file that did not exist — and that is
precisely the defect this file is for. **It is also why there is no allowlist.** An allowlist written
while that citation was false would have contained `fabric/deploy.py`, and the test would have been
green for the entire period during which the repository was lying. The exceptions here are derived
from `.gitignore` and from the shape of the citation itself; a name never buys its way out by being
named.

Three checks, and they are deliberately of different strengths:

1. **A path into this repository resolves.** "Into this repository" means the citation has a
   directory component whose first segment is a real top-level directory here — `tools/tmdl.py`,
   `src/warehouse/procs/`, `semantic-model/measures.dax`. This is the class the whole
   cross-referencing habit is built from, it is the class that actually went wrong, and it is the
   class a rename silently breaks.

2. **Every markdown link target resolves**, relative to the document that holds it. A broken
   `[text](path)` is the same defect wearing different syntax.

3. **A bare filename that does not resolve does not share its stem with a file that exists.** This
   is the weak one, and the asymmetry is the point. `parameter.yml` and `notebook-settings.json` are
   *Fabric's* filenames, correctly absent from a repository that does not contain a Fabric tenant;
   `conftest.pyx` would be a typo. Nothing in either name says which, because whose filesystem a
   name refers to is not a property of the name. What *is* mechanical: if the stem matches something
   here and the full name does not, then either the extension is wrong or the file moved, and both
   are defects. So the rule catches the near-misses and lets the genuinely-foreign names through.

**What this cannot catch.** Two things, both in check 3. A bare filename this repository once held
and has since renamed away reads exactly like one of Fabric's, and passes. And a citation whose
extension this repository does not author — `conftest.pyx` — is discarded before the stem is looked
at, by the same derived extension set that keeps `sys.argv` and `Layer.META` out. Widening it to
catch that would start reading `scd2.merge` as a misspelling of `src/lib/scd2.py`, and a test that
cries wolf about method references is a test people learn to ignore. The defence against both is
check 1: cite a file of this repository's own with its directory, as every document here does, and a
rename or a wrong extension fails the suite.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Directories holding nothing of ours: virtualenvs, caches, the local lake, and packaging metadata.
SKIP_DIRS = frozenset({".venv", "venv", "_onelake", "__pycache__", ".git", ".ruff_cache",
                       ".pytest_cache", "site-packages", "spark-warehouse", "metastore_db"})

# Files whose prose is scanned. `.py` is included because backticks appear in Python only inside
# comments and docstrings, which is exactly where the module-level citations live.
SCANNED = ("*.md", "*.py")

# A backticked token with an extension: `tools/tmdl.py`, `Makefile` is not one, `ms.date` is filtered
# by EXTENSIONS below. Trailing punctuation cannot occur — the closing backtick bounds it.
CITATION = re.compile(r"`([^`\s]+\.[A-Za-z0-9]{1,5})`")

# `[text](target)`, the other way a document points at a file.
MD_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")

# Stripped before scanning, so that `learn.microsoft.com/fabric/cicd/...` is not read as a path.
# Bare hostnames matter as much as full URLs: this repo cites Learn pages by path, without a scheme.
URL = re.compile(r"\bhttps?://\S+|\b[a-z0-9.-]+\.(?:com|io|org|net|dev|ai)/\S*")


def _generated_prefixes() -> tuple[str, ...]:
    """Paths `.gitignore` says are build output, read from it rather than repeated here.

    A citation of `fabric/build/` or `dashboard/out/` is a citation of something that exists only
    after `make fabric-build` or `make dashboard` has run. Asserting on them would make this test
    pass or fail on whether someone had run a build, which is not a property of the documentation —
    and would fail on the CI runner that has not.

    The same prefixes are excluded from the inventory below, and that matters more than it looks:
    `.html` and `.pbism` exist in this repo *only* inside build output, so a version of this test
    that inventoried them would derive a wider extension set on a developer's machine than on a cold
    runner. A check whose strictness depends on what you last built is not a check.
    """
    prefixes = []
    for line in (ROOT / ".gitignore").read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or not line or line.startswith("*") or "/" not in line.rstrip("/"):
            continue
        prefixes.append(line.rstrip("/") + "/")
    assert prefixes, ".gitignore no longer declares any directory of build output; this test's " \
                     "exemptions were derived from it and are now derived from nothing."
    return tuple(prefixes)


GENERATED = _generated_prefixes()


def _repo_files() -> list[Path]:
    return [
        p
        for p in ROOT.rglob("*")
        if p.is_file()
        and not any(part in SKIP_DIRS for part in p.relative_to(ROOT).parts)
        and ".egg-info" not in p.relative_to(ROOT).parts[0]
        and not p.relative_to(ROOT).as_posix().startswith(GENERATED)
    ]


REPO_FILES = _repo_files()
BASENAMES = {p.name for p in REPO_FILES}
STEMS = {p.name.rsplit(".", 1)[0] for p in REPO_FILES}
TOP_DIRS = {p.name for p in ROOT.iterdir() if p.is_dir() and p.name not in SKIP_DIRS}

# The extensions this repository actually authors. Everything else backticked-with-a-dot is a dotted
# identifier rather than a filename — `mssparkutils.notebook.run`, `Layer.META`, `sys.argv`,
# `transactions.amount_minor.range` — and deriving the set rather than listing it means a new kind of
# file starts being checked on the day one appears, without anyone remembering to add it.
EXTENSIONS = {p.name.rsplit(".", 1)[1] for p in REPO_FILES if "." in p.name}

SCAN_FILES = sorted(
    p
    for pattern in SCANNED
    for p in ROOT.rglob(pattern)
    if not any(part in SKIP_DIRS for part in p.relative_to(ROOT).parts)
    and not p.relative_to(ROOT).as_posix().startswith(GENERATED)
)
SCAN_IDS = [p.relative_to(ROOT).as_posix() for p in SCAN_FILES]


def _citations(path: Path) -> list[tuple[str, int]]:
    """Every backticked filename in a file, with its line number."""
    found = []
    for lineno, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        for match in CITATION.finditer(URL.sub(" ", line)):
            token = match.group(1)
            if token.rsplit(".", 1)[1] in EXTENSIONS:
                found.append((token, lineno))
    return found


def _resolves(token: str) -> bool:
    """Whether a citation names something on disk, honouring the `*` the repo map uses."""
    return bool(list(ROOT.glob(token)))


@pytest.mark.parametrize("path", SCAN_FILES, ids=SCAN_IDS)
def test_every_cited_path_into_this_repo_resolves(path: Path) -> None:
    """The check that would have failed for as long as `fabric/deploy.py` was only promised."""
    broken = [
        f"line {lineno}: `{token}`"
        for token, lineno in _citations(path)
        if "/" in token
        and token.split("/")[0] in TOP_DIRS
        and not token.startswith(GENERATED)
        and not _resolves(token)
    ]
    assert not broken, (
        f"{path.relative_to(ROOT)} cites paths into this repository that do not exist: {broken}. "
        f"Either the file was renamed and the citation was not, or the citation is a promise. A "
        f"promise belongs in the future tense with the word that says so; a path in backticks reads "
        f"as a fact."
    )


@pytest.mark.parametrize(
    "path", [p for p in SCAN_FILES if p.suffix == ".md"],
    ids=[i for i in SCAN_IDS if i.endswith(".md")],
)
def test_every_markdown_link_resolves(path: Path) -> None:
    """A broken `[text](path)` is the same defect as a broken backtick, in different syntax."""
    broken = []
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        for match in MD_LINK.finditer(line):
            target = match.group(1).split("#")[0]
            if not target or target.startswith(("http", "mailto:")):
                continue
            if target.startswith(GENERATED) or (path.parent / target).exists():
                continue
            broken.append(f"line {lineno}: ({target})")
    assert not broken, (
        f"{path.relative_to(ROOT)} links to files that do not exist: {broken}. On GitHub these "
        f"render as links and 404 on click, which is worse than plain text would have been."
    )


def test_no_bare_filename_citation_is_a_near_miss() -> None:
    """The weak check, and the reason it is weak is stated in this module's docstring.

    A bare filename with no directory cannot be attributed to a filesystem by inspection.
    `parameter.yml`, `pipeline-content.json`, `notebook-settings.json` and `fs-settings.json` are
    Fabric's own filenames, named in `fabric/README.md` precisely because this repository does *not*
    contain them; `conftest.pyx` would be a typo for something it does. The mechanical part is the
    stem: if a file here is called `conftest.py`, then `conftest.pyx` is either a wrong extension or
    a moved file, and no amount of context makes it a Fabric filename.
    """
    offenders: dict[str, list[str]] = {}
    for path in SCAN_FILES:
        for token, lineno in _citations(path):
            if "/" in token or "*" in token or _resolves(token) or token in BASENAMES:
                continue
            if token.rsplit(".", 1)[0] in STEMS:
                offenders.setdefault(token, []).append(
                    f"{path.relative_to(ROOT)}:{lineno}"
                )
    assert not offenders, (
        f"these citations name a file that does not exist, but share a stem with one that does: "
        f"{offenders}. That is a wrong extension or a file that moved, not a foreign filename."
    )


def test_the_scan_actually_reaches_the_documents_it_claims_to() -> None:
    """A citation test that silently scanned nothing would pass, which makes this assertion the
    load-bearing one.

    The counts are lower bounds rather than exact figures on purpose: an exact count is a second
    place to record how many documents the repo has, and it would fail on the commit that adds a
    page rather than on the commit that breaks something.
    """
    markdown = [p for p in SCAN_FILES if p.suffix == ".md"]
    assert len(markdown) >= 10, f"only {len(markdown)} markdown files scanned; the repo has more"
    cited = {token for p in SCAN_FILES for token, _ in _citations(p)}
    into_repo = {c for c in cited if "/" in c and c.split("/")[0] in TOP_DIRS}
    assert len(into_repo) >= 50, (
        f"only {len(into_repo)} paths into this repo were recognised as citations. The regex, the "
        f"URL filter or the derived extension set has stopped matching how the docs are written, "
        f"and this suite is now agreeing with itself rather than reading them."
    )
    assert "fabric/deploy.py" in into_repo, (
        "the citation that motivated this file is no longer being seen by it"
    )
