"""Generate `semantic-model/measures.dax` from the TMDL, and check it has not drifted.

**The TMDL is authoritative and this file is derived**, which is the whole point. Measures could
just as easily have been written in a `.dax` file and copied into the TMDL, and that arrangement
fails the same way every duplicated definition fails: one copy gets fixed and the other keeps being
read. TMDL is the format Fabric's git integration actually stores, so TMDL is the copy that has to
win, and `--check` wired into `make lint` is what stops the derived copy from quietly falling
behind.

So why have the derived copy at all?

  1. **There is nowhere else to read the model's semantics in one sitting.** The 36 measures are
     spread across six table files and interleaved with ~90 column declarations, partition blocks
     and annotations. A reviewer who wants to know what this model *means* should not have to
     assemble that by hand.
  2. **It is a runnable DAX query.** The generated `EVALUATE` returns every measure at the grand
     total, so on a real tenant this file is the first thing to execute against the model: paste,
     run, and every measure that fails to resolve names itself. That is a genuinely useful thing to
     have ready in advance, given that nothing here has been executed.
  3. `src/warehouse/procs/09_sp_load_agg_merchant_daily.sql` points a reader at it for the measures
     that consume the aggregate.

Three choices worth stating, because each is a place a generator usually goes wrong:

**Only the first paragraph of each measure's `///` documentation is carried over.** Copying all of
it would make this file a second copy of the TMDL with none of the metadata, and a file with no
reason to exist except mechanical completeness. The point is an index: enough rationale to know why
a measure is shaped the way it is, and a pointer to the TMDL for the argument in full.

**`formatString` and `displayFolder` travel as comments, not as syntax.** They are model metadata
rather than query syntax; a `DEFINE MEASURE` in a DAX query has no place to put them. Emitting them
as comments keeps the file paste-able, which is condition 2 above, and keeps them visible, which is
why they are here at all — a rate measure with the wrong format string is a real defect and not
obvious from the expression.

**Nothing in the output depends on the time or the machine.** No generation timestamp, no absolute
paths, no dictionary iteration order that is not explicitly sorted. A generated file that is not a
pure function of its input cannot be checked into git and verified in CI, because `--check` would
fail on a clean tree.

Usage:

    python tools/extract_dax.py            # write semantic-model/measures.dax
    python tools/extract_dax.py --check    # exit 1 with a diff if it is stale
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools import tmdl  # noqa: E402  (after the path insert, so the script runs from anywhere)

OUT = Path(__file__).resolve().parent.parent / "semantic-model" / "measures.dax"

WIDTH = 96

# Tables in reading order rather than in `sorted()` order. The three fact-ish tables carry 32 of
# the 36 measures and are what the model is for; the dimension measures are counts that only make
# sense once you have seen them. Anything not named here follows alphabetically, so adding a table
# with measures degrades to a stable order instead of raising.
TABLE_ORDER = ["fact_transaction", "fact_dispute", "agg_merchant_daily"]

# A `///` line that is only dashes or equals signs is a section divider inside the TMDL, laid out to
# be read in place. It carries no information once the surrounding layout is gone.
_DIVIDER = re.compile(r"^[-=]+$")


def _first_paragraph(doc: str) -> tuple[str | None, list[str]]:
    """Split a TMDL doc comment into `(section heading, first paragraph)`.

    A TMDL doc comment is a `///` block; a blank `///` line separates paragraphs within it, while a
    genuinely empty line ends the block and the parser drops it. Taking only the first paragraph is
    deliberate — see the module docstring.

    The heading needs pulling out separately because of how the TMDL is laid out: the first measure
    of each group is preceded by a `--- / title / ---` banner that introduces the whole group, and
    the parser hands that over as part of that one measure's documentation. Dropping the dividers
    and running the rest through one wrap glues the banner title onto the sentence after it, which
    reads as a garbled sentence rather than as a heading. So the title comes back on its own.
    """
    lines = [ln.strip() for ln in doc.splitlines()]
    heading: str | None = None
    if len(lines) >= 3 and _DIVIDER.match(lines[0]) and _DIVIDER.match(lines[2]):
        heading, lines = lines[1], lines[3:]

    out: list[str] = []
    for line in lines:
        if _DIVIDER.match(line):
            continue
        if not line:
            if out:
                break
            continue
        out.append(line)
    return heading, out


def _comment(lines: list[str], indent: str) -> list[str]:
    """Re-wrap prose as `//` comments at `indent`.

    Re-wrapping rather than reproducing the TMDL's line breaks, because the TMDL wraps to its own
    indentation and this file has a different one; keeping the original breaks would leave a ragged
    right margin that looks like a mistake.
    """
    if not lines:
        return []
    body = textwrap.fill(
        " ".join(lines),
        width=WIDTH - len(indent) - 3,
        break_long_words=False,
        break_on_hyphens=False,
    )
    return [f"{indent}// {line}".rstrip() for line in body.splitlines()]


def _measure_lines(table: str, name: str, block: tmdl.Block) -> list[str]:
    indent = " " * 4
    heading, prose = _first_paragraph(block.doc)
    out: list[str] = []
    if heading:
        out += [f"{indent}// {heading}", f"{indent}//"]
    out += _comment(prose, indent)

    meta = [f"{k}: {block.props[k]}" for k in ("formatString", "displayFolder") if k in block.props]
    if meta:
        out.append(f"{indent}// [{'  ·  '.join(meta)}]")

    expr = (block.value or "").strip()
    if "\n" in expr:
        out.append(f"{indent}MEASURE {table}[{name}] =")
        out += [f"{indent}{' ' * 4}{line}".rstrip() for line in expr.splitlines()]
    elif len(f"{indent}MEASURE {table}[{name}] = {expr}") <= WIDTH:
        out.append(f"{indent}MEASURE {table}[{name}] = {expr}")
    else:
        # Long enough that the `= expr` tail would run past the margin. Breaking after the `=` is
        # the conventional DAX formatting for this and keeps the measure name at the left edge
        # where it can be scanned for.
        out.append(f"{indent}MEASURE {table}[{name}] =")
        out.append(f"{indent}{' ' * 4}{expr}")
    return out


def _rule(text: str = "") -> str:
    """One header line: a full-width `=` rule when empty, `// text` otherwise, and a bare `//` for a
    single space. Three cases rather than two because a paragraph break inside a comment block wants
    an empty comment line, not a rule — a rule between every paragraph reads as five separate
    banners instead of one."""
    if text == " ":
        return "//"
    return f"// {'=' * (WIDTH - 3)}" if not text else f"// {text}"


def render(model: tmdl.Model) -> str:
    measures = model.measures  # raises on a model-wide measure-name collision
    by_table: dict[str, list[tuple[str, tmdl.Block]]] = {}
    for name, (table, block) in measures.items():
        by_table.setdefault(table, []).append((name, block))

    ordered = [t for t in TABLE_ORDER if t in by_table]
    ordered += sorted(t for t in by_table if t not in ordered)

    lines = [
        _rule(),
        _rule("measures.dax — GENERATED. Do not edit this file."),
        _rule(" "),
        _rule("Generated from semantic-model/definition/tables/*.tmdl by tools/extract_dax.py."),
        _rule("`make lint` runs that script with --check and fails if this file is stale, so the"),
        _rule("TMDL is the one copy that can be edited and this one cannot drift away from it."),
        _rule(" "),
        _rule("This is a complete DAX query: a DEFINE section holding every measure in the model,"),
        _rule("followed by an EVALUATE that returns all of them at the grand total. Pasted into"),
        _rule("DAX query view or Tabular Editor against the deployed model, it is the fastest way"),
        _rule("to find a measure that does not resolve — the error names it. Nothing in this repo"),
        _rule("has executed it; see semantic-model/README.md."),
        _rule(" "),
        _rule("Each measure carries the first paragraph of its TMDL documentation and its format"),
        _rule("string and display folder, as comments. The full reasoning stays in the TMDL beside"),
        _rule("the measure, which is the file to read and the only file to edit."),
        _rule(),
        "",
        "DEFINE",
    ]

    for table in ordered:
        lines += [
            "",
            f"    // {'-' * (WIDTH - 7)}",
            f"    // {table}  ({len(by_table[table])} measure"
            f"{'s' if len(by_table[table]) != 1 else ''})",
            f"    // {'-' * (WIDTH - 7)}",
        ]
        for name, block in sorted(by_table[table], key=lambda p: p[1].line):
            lines.append("")
            lines += _measure_lines(table, name, block)

    # Every measure, at the grand total. Generated rather than curated so it cannot fall behind the
    # model: a measure added to the TMDL and forgotten here would be exactly the kind of thing this
    # query exists to catch.
    lines += [
        "",
        "",
        f"// {'-' * (WIDTH - 3)}",
        "// Smoke query: every measure above, evaluated with no filter context. A measure that",
        "// returns BLANK here may be correct — several are guarded and are meant to. A measure",
        "// that raises is not.",
        f"// {'-' * (WIDTH - 3)}",
        "EVALUATE",
        "ROW (",
    ]
    names = [n for t in ordered for n, _ in sorted(by_table[t], key=lambda p: p[1].line)]
    for j, name in enumerate(names):
        comma = "," if j < len(names) - 1 else ""
        lines.append(f'    "{name}", [{name}]{comma}')
    lines.append(")")

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit 1 with a diff if the file on disk is stale",
    )
    args = ap.parse_args(argv)

    want = render(tmdl.load())

    if not args.check:
        OUT.write_text(want)
        n = want.count("MEASURE ")
        print(f"extract-dax: wrote {OUT.relative_to(OUT.parents[1])} — {n} measures")
        return 0

    have = OUT.read_text() if OUT.exists() else ""
    if have == want:
        print(f"extract-dax: OK — {want.count('MEASURE ')} measures, no drift")
        return 0

    rel = OUT.relative_to(OUT.parents[1])
    sys.stdout.writelines(
        difflib.unified_diff(
            have.splitlines(keepends=True),
            want.splitlines(keepends=True),
            fromfile=f"{rel} (on disk)",
            tofile=f"{rel} (from TMDL)",
        )
    )
    print(
        f"\nextract-dax: {rel} is stale. The TMDL is authoritative — regenerate with\n"
        f"    python tools/extract_dax.py\n"
        f"and commit the result. Do not hand-edit {rel}.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
