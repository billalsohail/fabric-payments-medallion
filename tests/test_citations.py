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
from collections import Counter
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


# --- Counts stated in prose -------------------------------------------------------------------
#
# The checks above verify that a cited *path* exists. These verify that a cited *count* is right,
# which is the same class of defect one level down: `docs/design-decisions.md` opened with "Ten
# decisions" for as long as it had fourteen, and said so two paragraphs above a sentence reading
# "Decisions 11-14 came out of building rather than out of planning." Nothing was wrong with either
# sentence alone. The page disagreed with itself, and with `README.md`, which had the right number.
#
# Only counts that are (a) derivable from the repo and (b) written out in more than one place are
# worth pinning. A count stated once can be read against the thing it counts; a count stated three
# times is an invitation to drift, and the number of design decisions is now stated in this file too,
# which is the point — it is stated here *as a derivation*, not as a literal.

# Number words this repo's prose actually uses, as an allowlist rather than a denylist of every
# other adjective. A denylist was tried first and immediately flagged "numbered decisions" — an
# adjective, not a count — in two places including this file's own assertion message. Matching only
# things that *are* numbers cannot make that mistake, and the cost is that a page spelling out
# "twenty" starts passing silently rather than failing; the NUMERALS assertion below is what keeps
# that from being invisible.
NUMERALS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
    "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
}


def _decision_headings() -> int:
    """`## N. <title>` in the decisions page — the thing every stated count has to agree with."""
    text = (ROOT / "docs" / "design-decisions.md").read_text()
    numbers = [int(m.group(1)) for m in re.finditer(r"^## (\d+)\.", text, re.M)]
    assert numbers == list(range(1, len(numbers) + 1)), (
        f"design decisions are numbered {numbers}, which is not 1..{len(numbers)}. The opening "
        f"paragraph calls the numbers a stable interface that does not get reordered, and files "
        f"outside docs/ cite them, so a gap or a repeat is a broken reference somewhere."
    )
    return len(numbers)


def test_every_stated_count_of_design_decisions_matches_the_headings() -> None:
    """The defect this pair of tests exists for, in the file it happened in."""
    n = _decision_headings()
    assert n in NUMERALS.values(), (
        f"there are now {n} design decisions, which NUMERALS cannot spell. Extend it, or this test "
        f"stops being able to read the sentence it is checking."
    )
    wrong: dict[str, list[str]] = {}
    for path in SCAN_FILES:
        for lineno, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            for m in re.finditer(r"\b([A-Za-z]+|\d{1,3}) (decisions|trade-offs)\b", line):
                said = m.group(1).lower()
                value = NUMERALS.get(said, int(said) if said.isdigit() else None)
                if value is None or value == n:
                    continue
                wrong.setdefault(f"{said} {m.group(2)}", []).append(
                    f"{path.relative_to(ROOT)}:{lineno}"
                )
    assert not wrong, (
        f"docs/design-decisions.md has {n} numbered decisions, but these say otherwise: {wrong}. "
        f"Append a decision and this fails until every page that counts them agrees — which is the "
        f"only reason the count is safe to write out in prose at all."
    )


def test_the_decision_numbers_cited_elsewhere_exist() -> None:
    """`#12` in a docstring is a link. It should not point past the end of the page."""
    n = _decision_headings()
    dangling: dict[str, list[str]] = {}
    for path in SCAN_FILES:
        for lineno, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            if "decision" not in line.lower() and "#" not in line:
                continue
            for m in re.finditer(r"(?<![\w#])#(\d{1,3})(?![\d\w])", line):
                cited = int(m.group(1))
                if cited < 1 or cited > n:
                    dangling.setdefault(f"#{cited}", []).append(
                        f"{path.relative_to(ROOT)}:{lineno}"
                    )
    assert not dangling, (
        f"these cite a design decision that does not exist (the page has {n}): {dangling}. Either "
        f"the decision was removed and its citation was not, or the number is a typo — and because "
        f"the numbers are a stable interface, renumbering to fix it would break the others."
    )


# Ways of asking pytest for a subset, by the dest of the option that requests one. `keyword` (-k) and
# `markexpr` (-m) come from pytest's core argument parsing and are always present; `lf`, `failedfirst`
# and `deselect` come from plugins and are read defensively, because a dest is not a documented
# interface. `_narrowing_option` asserts the stable two exist so that a rename cannot quietly turn
# the skip guard into a no-op.
_CORE_NARROWING = ("keyword", "markexpr")
_PLUGIN_NARROWING = ("lf", "failedfirst", "deselect")


def _narrowing_option(config: pytest.Config) -> str | None:
    """The name of the option that narrowed collection, or None if the whole suite was asked for."""
    missing = [n for n in _CORE_NARROWING if not hasattr(config.option, n)]
    assert not missing, (
        f"pytest no longer exposes {missing} on config.option, so this test can no longer tell a "
        f"full run from a subset. It would skip nothing and compare the suite's stated size against "
        f"whatever fraction happened to be selected."
    )
    for name in (*_CORE_NARROWING, *_PLUGIN_NARROWING):
        if getattr(config.option, name, None):
            return name
    return None


def test_every_stated_test_count_matches_the_suite(request: pytest.FixtureRequest) -> None:
    """The suite's size is written out in six documents, so it is six places that go stale at once.

    The count comes from pytest's own collection rather than from counting `def test_`, because
    parametrisation over `SCAN_FILES` and over the `.sql` files means the suite's size is a property
    of the repository's *contents*, not of the test code — there are far more tests than there are
    test functions, and anything counting functions would be wrong by more than a hundred.

    This docstring deliberately contains no digits followed by the word this test greps for. An
    earlier draft opened by quoting the stale figure and flagged itself, which is the second time
    writing these checks that the check caught its own prose — see the note on `NUMERALS` above.

    **Not every stated count is the suite's.** The docs also state per-area figures — how many tests
    hold the linter, the semantic model, the `fabric/` layer — and those are as worth pinning as the
    total. So the allowlist is the total *plus every individual test file's own count*, all from the
    same collection, and a figure is wrong when it matches none of them. The first draft compared
    everything against the total alone, which would have failed on three true sentences.

    What that cannot catch, stated because the weakness is real: if one file's stated count goes
    stale and the number it went stale at happens to equal some other file's count, this passes. The
    case it does catch is the one that actually happens — the suite grows, the total matches nothing
    any more, and every page quoting it fails at once.

    **Skipped when a subset is running**, which is not a loophole but the only correct behaviour:
    `pytest -k citations` collects a fraction of the suite, and neither that fraction nor the
    per-file counts derived from it are what any document is claiming.

    The first version of the skip guard read `config.option.last_failed` and died with an
    `AttributeError` on the full run — pytest's `--last-failed` has the dest `lf`, and `last_failed`
    is not an attribute of anything. Two things came out of fixing it. Attribute names on
    `config.option` are pytest's private surface, so they are read through `getattr` with a default
    rather than assumed; and because a `getattr` default turns a renamed option into a guard that
    silently never fires, `_narrowing_option` asserts the stable ones are actually present. A guard
    that cannot fire is the failure mode this repo keeps writing tests against, and it very nearly
    shipped inside one of them.
    """
    config = request.config
    if narrowed := _narrowing_option(config):
        pytest.skip(f"--{narrowed} selected a subset; the collected count is not the suite's count")
    if [Path(a).resolve() for a in config.args] != [ROOT / "tests"]:
        pytest.skip(f"collection was narrowed to {config.args}; not the whole suite")

    per_file = Counter(item.nodeid.split("::")[0] for item in request.session.items)
    collected = len(request.session.items)
    legitimate = {collected} | set(per_file.values())
    assert collected in legitimate and len(per_file) >= 10, (
        f"collection returned {collected} tests across {len(per_file)} files, which is not a whole "
        f"suite. This check is comparing prose against something other than what it claims to."
    )

    stated: dict[str, list[str]] = {}
    for path in SCAN_FILES:
        for lineno, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            for m in re.finditer(r"\b(\d{2,4}) tests\b", line):
                if int(m.group(1)) not in legitimate:
                    stated.setdefault(m.group(1), []).append(f"{path.relative_to(ROOT)}:{lineno}")
    assert not stated, (
        f"these state a test count that is neither the suite's ({collected}) nor any single test "
        f"file's: {stated}. Counts in the suite right now are "
        f"{dict(sorted(per_file.items(), key=lambda kv: -kv[1]))}. Adding a test is supposed to "
        f"change one of these numbers, so the fix is to update the prose — and the reason this check "
        f"exists is that the total is written in six places and nobody edits six places."
    )
