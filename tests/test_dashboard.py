"""The dashboard's contracts with the semantic model — the half that needs no engine.

`dashboard/build_dashboard.py` is a second implementation of the model's 36 measures, which the
repo otherwise refuses to allow. Its module docstring names the three fences that make the
duplication survivable; two of them are executable, and this file executes them.

**The rule these tests exist to pin was learned the hard way.** Every money measure in this model
once summed a minor-unit column under a `\\£#,0.00` format string, and two TMDL files claimed the
format string divided by 100. It cannot — a DAX format string formats, and the only scaling it can
express is the trailing comma's thousands. All 23 semantic-model tests passed anyway, because a
format string that formats the wrong magnitude is consistent with everything: the column resolves,
the measure resolves, nothing divides with a slash. The defect was found by *rendering* the model
and looking at the number, which is what the dashboard is for.

Having found it that way once, the repo should not need to find it that way again. So
`test_a_measure_sums_minor_units_exactly_when_it_divides_by_100` states the rule the fix
established, in both directions, as something that fails on a push rather than something a reader
has to notice. A comment claiming a conversion happens is what got us here; a test asserting it is
the replacement.

The numeric half of plan verification item 8 — dashboard figures cross-checked against independent
SQL — is in `tests/test_gold_recon.py`, under "The dashboard's arithmetic". It lives there because
it needs a loaded warehouse and that file already has one; loading a second identical warehouse to
put the tests in a file named after the thing they check would cost two minutes a run and prove
nothing extra. Nothing here touches Spark, so the whole file runs in milliseconds.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))

import build_dashboard as dash  # noqa: E402  (after the path insert; dashboard/ is not a package)

from tools import tmdl  # noqa: E402

MONEY = r"\£#,0.00;(\£#,0.00);\£#,0.00"

# `DIVIDE(<anything>, 100)` — a literal 100 in a denominator position. Deliberately not a match for
# `* 100`, which `[3DS Approval Uplift (pp)]` uses to express percentage points and which is not a
# unit conversion.
_DIVIDES_BY_100 = re.compile(r",\s*100\s*\)")


@pytest.fixture(scope="module")
def model() -> tmdl.Model:
    return tmdl.load()


# --------------------------------------------------------------------------------------
# Minor units: the rule is that a stored column is an integer count and a £ sign means
# a measure has converted it
# --------------------------------------------------------------------------------------

def test_every_minor_column_is_formatted_as_an_integer(model):
    """A `_minor` column holds an integer count of minor units, so it is formatted as one.

    The warehouse stores minor units because integer pence are exact and a decimal pound column
    accumulates float error over a sum of millions of rows. A money format string on such a column
    renders a figure exactly 100x too large with a currency symbol in front of it, which is the
    worst available outcome: right digits, wrong magnitude, and it looks considered.
    """
    bad = []
    for table_name, table in sorted(model.tables.items()):
        for col in table.of_kind("column"):
            if col.name and col.name.endswith("_minor"):
                fmt = col.props.get("formatString")
                if fmt != "#,0":
                    bad.append(f"{table_name}[{col.name}] is formatted {fmt!r}, want '#,0'")
    assert not bad, (
        "a `_minor` column must be formatted as an integer; a £ sign belongs only on a measure "
        "that has divided by 100. See the minor-units note in dim_currency.tmdl.\n  "
        + "\n  ".join(bad)
    )


def test_a_measure_sums_minor_units_exactly_when_it_divides_by_100(model):
    """The conversion rule, in both directions, over every measure in the model.

    Forwards: a measure that references a `_minor` column is summing minor units and must divide by
    100, or it reports pence as pounds. Backwards: a measure that divides by 100 without touching a
    `_minor` column is scaling something that was never in minor units, which is the same defect
    mirrored — and the more likely one once this rule is known, because dividing by 100 starts to
    look like what money measures do.

    Composed measures satisfy both sides by referencing neither: `[Average Approved Value (GBP)]`
    divides two measures whose numerator is already in pounds, and `[Chargeback Volume Rate (bps)]`
    is a ratio of two money measures where the 1/100 cancels. Both are money-formatted and neither
    divides by 100, which is correct, and this test is the reason that can be stated as correct
    rather than merely asserted in a comment.
    """
    wrong = []
    for name, (table, block) in sorted(model.measures.items()):
        expr = block.value or ""
        sums_minor = any(col.endswith("_minor") for _, col in tmdl.column_refs(expr))
        converts = bool(_DIVIDES_BY_100.search(expr))
        if sums_minor and not converts:
            wrong.append(
                f"{name} ({table}) sums a `_minor` column and does not divide by 100 — it reports "
                "minor units as though they were pounds"
            )
        elif converts and not sums_minor:
            wrong.append(
                f"{name} ({table}) divides by 100 but sums no `_minor` column — it is scaling "
                "something that was not in minor units"
            )
    assert not wrong, "\n  ".join(["measure conversions do not match their inputs:"] + wrong)


def test_the_money_measures_are_the_ones_that_convert(model):
    """And the £ sign lands only on measures that end up in pounds.

    The two tests above say a conversion happens where minor units are summed. This one says the
    formatting agrees: every money-formatted measure either converts, or is built from measures
    that do. A measure formatted as money that does neither is the original defect returning under
    a different name.
    """
    money = {n for n, (_, b) in model.measures.items() if b.props.get("formatString") == MONEY}
    assert money, "no money-formatted measures found — has the format string changed?"
    for name in sorted(money):
        _, block = model.measures[name]
        expr = block.value or ""
        if _DIVIDES_BY_100.search(expr):
            continue
        refs = tmdl.measure_refs(expr)
        assert refs & money, (
            f"{name} is formatted as money but neither divides by 100 nor references a money "
            f"measure. Its expression is {' '.join(expr.split())!r}, which means the £ sign on it "
            "is a claim nothing backs."
        )


# --------------------------------------------------------------------------------------
# Fence 3: the formatting is not reimplemented, so the renderer and the model must agree
# on exactly which format strings exist
# --------------------------------------------------------------------------------------

def test_format_renders_every_format_string_the_model_uses(model):
    """No measure in the TMDL can carry a format string the dashboard cannot render.

    `_format` raises on an unknown format string rather than falling back to the raw number, which
    is the right behaviour at build time and a build failure at an inconvenient moment. This turns
    it into a test failure at a convenient one.
    """
    unrenderable = []
    for name, (_, block) in sorted(model.measures.items()):
        fmt = block.props.get("formatString")
        assert fmt, f"measure {name} has no formatString"
        try:
            dash._format(1234.5, fmt)
        except ValueError as exc:
            unrenderable.append(f"{name}: {exc}")
    assert not unrenderable, "\n  ".join(["format strings with no renderer:"] + unrenderable)


def test_every_renderer_is_reachable_from_the_model(model):
    """And the converse: the dashboard declares no format string the model has stopped using.

    A stale constant here is harmless to the output and corrosive to the file — it reads as though
    the model still formats something that way, and the next person adds a sixth case by analogy
    with a case that is dead.
    """
    used = {b.props.get("formatString") for _, b in model.measures.values()}
    declared = {
        "_MONEY": dash._MONEY,
        "_PERCENT": dash._PERCENT,
        "_INT": dash._INT,
        "_ONE_DP": dash._ONE_DP,
        "_SIGNED": dash._SIGNED,
    }
    dead = sorted(k for k, v in declared.items() if v not in used)
    assert not dead, f"format strings declared in build_dashboard.py and used by no measure: {dead}"


# --------------------------------------------------------------------------------------
# Fence 2: a tile names a measure, and the name is checked
# --------------------------------------------------------------------------------------

def test_a_tile_naming_an_unknown_measure_raises(model):
    """The fence itself. A tile labelled with a measure the model does not have is worse than no
    tile: it reads as authoritative and is unattached to anything."""
    r = dash.Report(model)
    with pytest.raises(ValueError, match="no measure named"):
        r.tile("Authorization Rate", 0.9)  # American spelling; the model says Authorisation


def test_a_rendered_tile_is_counted_as_shown(model):
    """Coverage is a by-product of building the page rather than a number someone maintains, which
    is what lets the footer's "N of the 36 measures" be true without anyone keeping it true."""
    r = dash.Report(model)
    assert r.shown == set()
    r.tile("Authorisation Rate", 0.8592)
    assert r.shown == {"Authorisation Rate"}


# --------------------------------------------------------------------------------------
# The two helpers that stand in for DAX
# --------------------------------------------------------------------------------------

def test_divide_is_blank_not_infinity_on_a_zero_denominator():
    """`DIVIDE` semantics, for the reason fact_transaction.tmdl gives: a merchant with no attempts
    should vanish from an approval-rate chart rather than appear at the top of it."""
    assert dash.divide(1.0, 0) is None
    assert dash.divide(1.0, None) is None
    assert dash.divide(None, 2.0) is None
    assert dash.divide(3.0, 4.0) == 0.75


def test_pounds_converts_minor_units_and_propagates_blank():
    assert dash.pounds(6379) == 63.79
    assert dash.pounds(0) == 0.0
    assert dash.pounds(None) is None


def test_format_of_blank_is_an_em_dash():
    """A BLANK measure renders as an em dash and not as 0. `[Distinct Accounts (per day)]` is BLANK
    at the grand total by design, and a 0 there would read as "no accounts transacted"."""
    assert dash._format(None, dash._INT) == "—"


def test_money_formatting_is_the_format_string_and_nothing_more():
    """`_format` applies no scaling of its own — the property that let this page disagree with the
    model. 6379 in, £6,379.00 out. The conversion is `pounds()`'s job, as it is the measure's."""
    assert dash._format(6379, MONEY) == "£6,379.00"
    assert dash._format(dash.pounds(6379), MONEY) == "£63.79"
    assert dash._format(-1234.5, MONEY) == "(£1,234.50)"
