"""Build a static two-page HTML dashboard from the gold warehouse.

**This is a stand-in for the Direct Lake report, and not a report.** There is no tenant and no
Power BI Desktop on this machine, so `semantic-model/` has never been loaded by a DAX engine — see
the UNVALIDATED note in `semantic-model/README.md`. Twenty-three tests check that the model is
internally consistent and consistent with the warehouse DDL, which is a real guarantee and a narrow
one: it establishes that every reference resolves, and says nothing about whether the measures
produce sensible numbers. Nothing in this repo had ever produced a *number* from them.

That is what this file is for. It computes the same quantities by a different route — Spark SQL over
the gold Delta tables — and lays them out the way the two report pages would be laid out, so the
model's arithmetic can be eyeballed at a scale where wrong is obvious.

**The duplication that buys, stated plainly.** Everywhere else in this repo a definition lives in
exactly one place and anything that looks like a second copy is generated from the first
(`semantic-model/measures.dax`, checked by `make lint`). That is impossible here: DAX needs an
engine, so the arithmetic below is genuinely a second implementation of the model's measures, and
two implementations of the same thing drift. Three things keep that bounded:

  1. **Only the additive components are duplicated.** Every SQL query here sums stored columns —
     `SUM(is_approved)`, `SUM(amount_gbp_minor)`, `COUNT(*)`. Every ratio is then formed once in
     Python by `divide()`, and every minor-units-to-pounds conversion once by `pounds()`, both of
     which are the same `DIVIDE(...)` shapes the TMDL requires and argues for at length in
     `fact_transaction.tmdl` and `dim_currency.tmdl`. So the duplicated part is the mechanical part,
     and the part where a wrong answer hides — a rate averaged instead of recomputed — is not
     duplicated at all.
  2. **Every tile names the measure it stands in for**, and `Report.tile` looks that name up in the
     TMDL and raises if it is not there. A measure renamed in the model breaks this build rather
     than leaving a tile quietly mislabelled.
  3. **The formatting is not reimplemented either.** A tile takes no format argument: the format
     string comes from the measure's own `formatString` in the TMDL. `_format` implements that
     format string *faithfully and applies no scaling of its own* — see its docstring, because that
     choice is what makes this dashboard able to disagree with the model, which is the whole point
     of having it.

`tests/test_dashboard.py` then recomputes two measures by a third, independent SQL path, which is
verification item 8 of the project plan.

**What this cannot check**, because it is a static grand-total-and-trend page with no filter
context: anything whose behaviour *is* filter context. `[Distinct Accounts (per day)]` is rendered
blank below, which is correct and is the guard working, but a blank cell is not proof the guard
fires at the right grain — `tests/test_gold_recon.py` is what demonstrates that. The role-playing
date relationship on `fact_dispute` is invisible here for the same reason: with no date filter,
`USERELATIONSHIP` changes nothing, so `[Disputes Resolved]` and `[Dispute Resolution Days]` reduce
to their `is_open = 0` filter and the second date role is never exercised. Row-level security is
absent entirely; it is not written yet, and gap 1 of `semantic-model/README.md` says why.

No CDN, no JavaScript charting library, no web fonts. The output is one self-contained file that
renders identically from `file://` with the network off, now and in two years. The charts are inline
SVG and CSS bars for that reason, and the visual design is deliberately plain: the numbers are the
deliverable and anything else would be dressing up an artefact that is already labelled a stand-in.

Usage:

    python dashboard/build_dashboard.py            # writes dashboard/out/index.html
    python dashboard/build_dashboard.py --open     # and opens it
"""

from __future__ import annotations

import argparse
import html
import sys
import webbrowser
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.lib import gold  # noqa: E402  (after the path insert, so the script runs from anywhere)
from src.runtime.context import get_spark  # noqa: E402
from tools import tmdl  # noqa: E402

OUT = Path(__file__).resolve().parent / "out" / "index.html"


# -------------------------------------------------------------------------------------
# Arithmetic
# -------------------------------------------------------------------------------------
def divide(num: float | None, den: float | None) -> float | None:
    """`DIVIDE(num, den)` — BLANK on a zero or missing denominator, never an exception.

    The same rule `test_no_measure_divides_with_a_slash` enforces on every measure in the model, and
    here for the same reason: a merchant with no attempts should vanish from an approval-rate chart
    rather than appear at the top of it with an infinity. `None` is this file's BLANK, and `_format`
    renders it as an em dash.
    """
    if num is None or not den:
        return None
    return num / den


def pounds(minor: float | None) -> float | None:
    """`DIVIDE(SUM(<something>_minor), 100)` — minor units to pounds, as the measures do it.

    Every money measure that sums a `_minor` column divides by 100, because the warehouse stores
    integer minor units and a DAX format string cannot scale. `semantic-model/definition/tables/
    dim_currency.tmdl` argues that at length; this is the stand-in for it, and it is a named
    function rather than a `/ 100` at each call site so that the conversion is one thing that can
    be found, checked against the measures, and counted.

    Applying it here rather than inside `_format` is the whole point — see `_format`.
    """
    return divide(minor, 100)


# -------------------------------------------------------------------------------------
# Formatting, driven by the model's own format strings
# -------------------------------------------------------------------------------------
# A tile declares a measure name and nothing else; the format comes from that measure's
# `formatString` in the TMDL. So the model is the one place a format is written down, and these are
# the five format strings it uses. An unrecognised one raises: a fallback that printed the raw
# number would let a format change in the TMDL pass silently through the thing whose job is to
# notice that the model and the numbers disagree.
_MONEY = r"\£#,0.00;(\£#,0.00);\£#,0.00"
_PERCENT = "0.00%"
_INT = "#,0"
_ONE_DP = "#,0.0"
_SIGNED = "+0.00;-0.00;0.00"


def _format(value: float | None, format_string: str) -> str:
    """Render `value` as the TMDL's `format_string` would.

    **This applies no scaling of its own, deliberately**, and that decision is the reason this
    file was worth building. A format string formats; it cannot scale by 100. So when the warehouse
    stores minor units, the conversion has to be in the measure, and the tempting shortcut — divide
    by 100 here, where it is one character — would have made this page look right while the model
    stayed wrong, which is the one outcome that would have made building it pointless.

    That is not hypothetical. On its first run this function printed an average approved card
    payment of £6,379.00, because every money measure summed a minor-unit column under a `\£#,0.00`
    format string and a comment two files away claimed the format string divided by 100. It does
    not. The measures now divide, `pounds()` above is this file's stand-in for that division, and
    this function remains a faithful reading of the format string and nothing more — so the next
    time the model and the numbers disagree, the page will say so again.
    """
    if value is None:
        return "—"
    if format_string == _MONEY:
        neg = value < 0
        body = f"£{abs(value):,.2f}"
        return f"({body})" if neg else body
    if format_string == _PERCENT:
        return f"{value * 100:,.2f}%"
    if format_string == _INT:
        return f"{round(value):,}"
    if format_string == _ONE_DP:
        return f"{value:,.1f}"
    if format_string == _SIGNED:
        return f"{value:+,.2f}"
    raise ValueError(
        f"no renderer for format string {format_string!r}. It is new in the TMDL; add it here "
        "rather than letting this page print a number the model would have formatted differently."
    )


# -------------------------------------------------------------------------------------
# Reading gold
# -------------------------------------------------------------------------------------
def _rows(sql: str) -> list[dict]:
    return [r.asDict() for r in get_spark().sql(sql).collect()]


def _one(sql: str) -> dict:
    rows = _rows(sql)
    if len(rows) != 1:
        raise ValueError(f"expected one row, got {len(rows)}:\n{sql}")
    return rows[0]


@dataclass
class Gold:
    """Everything this page needs, read in one pass so the queries are all in one place."""

    txn: dict
    declines_by_reason: list[dict]
    declines_by_channel: list[dict]
    three_ds: list[dict]
    wallets: list[dict]
    monthly: list[dict]
    merchants: list[dict]
    disputes: dict
    dims: dict
    agg: dict


def read_gold() -> Gold:
    """Run every query. Nothing here loads gold — `make run` does that, and a dashboard that
    silently rebuilt the warehouse it is reporting on would be a dashboard you could not trust to be
    reporting on the run you just made."""
    gold.ensure_schema()
    attempts = _one("SELECT COUNT(*) AS attempts FROM dbo.fact_transaction")["attempts"]
    if not attempts:
        raise SystemExit(
            "dbo.fact_transaction is empty, so there is nothing to report on. Run `make run` "
            "first (or `make generate seed run` on a fresh clone)."
        )

    txn = _one("""
        SELECT COUNT(*)                                                  AS attempts,
               SUM(is_approved)                                          AS approved,
               SUM(is_declined)                                          AS declined,
               SUM(is_reversed)                                          AS reversed,
               SUM(is_settled)                                           AS settled,
               SUM(amount_gbp_minor)                                     AS attempted_gbp_minor,
               SUM(CASE WHEN is_approved = 1 THEN amount_gbp_minor END)  AS approved_gbp_minor,
               -- AVG ignores NULLs, which is what makes this settled-only without a WHERE clause.
               -- The measure leans on the same behaviour in DAX (AVERAGE ignores BLANK) and
               -- fact_transaction.tmdl says so explicitly, so the two agree by construction.
               AVG(CAST(settlement_lag_days AS DOUBLE))                  AS avg_settlement_lag,
               SUM(CASE WHEN fx_rate_is_carried THEN 1 ELSE 0 END)       AS fx_carried
        FROM dbo.fact_transaction
    """)

    # Retriable declines need the dimension, so this is its own query rather than a column above.
    txn["retriable_declines"] = _one("""
        SELECT SUM(CASE WHEN r.is_retriable THEN 1 ELSE 0 END) AS n
        FROM dbo.fact_transaction f
        JOIN dbo.dim_decline_reason r ON f.decline_reason_sk = r.decline_reason_sk
        WHERE f.is_declined = 1
    """)["n"]

    # `WHERE is_declined = 1` is what keeps the -1 "not applicable" member off this breakdown. The
    # DAX measure gets there differently — `IF(DeclinesInContext > 0, ...)` — because a measure has
    # no WHERE clause to put it in. Same rows, and dim_decline_reason.tmdl explains the DAX side.
    declines_by_reason = _rows("""
        SELECT r.decline_reason_code AS code,
               r.decline_reason_desc AS reason,
               r.decline_category    AS category,
               r.is_retriable        AS retriable,
               COUNT(*)              AS declines
        FROM dbo.fact_transaction f
        JOIN dbo.dim_decline_reason r ON f.decline_reason_sk = r.decline_reason_sk
        WHERE f.is_declined = 1
        GROUP BY 1, 2, 3, 4
        ORDER BY declines DESC
    """)

    declines_by_channel = _rows("""
        SELECT channel, COUNT(*) AS attempts, SUM(is_declined) AS declined
        FROM dbo.fact_transaction GROUP BY channel ORDER BY attempts DESC
    """)

    # ECOM only, on both sides. is_3ds is FALSE by construction off ECOM, so an unrestricted split
    # would put every ATM withdrawal in the "without" group — the failure the measure's own comment
    # in fact_transaction.tmdl calls out.
    three_ds = _rows("""
        SELECT is_3ds, COUNT(*) AS attempts, SUM(is_approved) AS approved
        FROM dbo.fact_transaction WHERE channel = 'ECOM' GROUP BY is_3ds
    """)

    # The NULL group is labelled rather than coalesced. Silver deliberately did not COALESCE this to
    # 'UNKNOWN' — that would have erased the difference between "not captured" and "no wallet" — so
    # naming it at the point of display is where the distinction survives and is still legible.
    wallets = _rows("""
        SELECT COALESCE(wallet_type, '(not recorded)') AS wallet,
               COUNT(*) AS attempts, SUM(is_approved) AS approved
        FROM dbo.fact_transaction GROUP BY 1 ORDER BY attempts DESC
    """)

    monthly = _rows("""
        SELECT d.year_month           AS ym,
               COUNT(*)               AS attempts,
               SUM(f.is_approved)     AS approved,
               SUM(f.is_declined)     AS declined,
               SUM(f.amount_gbp_minor) AS attempted_gbp_minor
        FROM dbo.fact_transaction f
        JOIN dbo.dim_date d ON f.date_sk = d.date_sk
        GROUP BY 1 ORDER BY 1
    """)

    # Grouped by merchant_id, not merchant_sk: the surrogate key identifies an SCD2 *version*, so
    # grouping by it would split a merchant that was re-onboarded or re-scored mid-period into two
    # rows of a "top merchants" table, which is never what that table means.
    merchants = _rows("""
        SELECT m.merchant_id, MAX(m.merchant_name) AS merchant_name,
               MAX(m.mcc_category) AS mcc_category,
               COUNT(*) AS attempts, SUM(f.is_approved) AS approved,
               SUM(f.amount_gbp_minor) AS attempted_gbp_minor
        FROM dbo.fact_transaction f
        JOIN dbo.dim_merchant m ON f.merchant_sk = m.merchant_sk
        GROUP BY m.merchant_id ORDER BY attempted_gbp_minor DESC LIMIT 10
    """)

    disputes = _one("""
        SELECT COUNT(*)                           AS raised,
               SUM(is_open)                       AS open_now,
               SUM(is_won)                        AS won,
               SUM(is_lost)                       AS lost,
               SUM(disputed_amount_gbp_minor)     AS disputed_gbp_minor,
               SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END)           AS resolved,
               AVG(CASE WHEN is_open = 0 THEN CAST(resolution_days AS DOUBLE) END)
                                                                      AS resolution_days
        FROM dbo.fact_dispute
    """)

    dims = _one("""
        SELECT (SELECT COUNT(DISTINCT account_id)  FROM dbo.dim_account  WHERE is_current) AS accts,
               (SELECT COUNT(DISTINCT customer_id) FROM dbo.dim_customer WHERE is_current) AS custs,
               (SELECT COUNT(DISTINCT merchant_id) FROM dbo.dim_merchant WHERE is_current) AS mrchs,
               (SELECT AVG(CAST(risk_score AS DOUBLE)) FROM dbo.dim_merchant WHERE is_current)
                                                                                          AS risk
    """)
    # The business key through the fact, which is what [Distinct Accounts] counts via SUMMARIZE.
    # Counting fact_transaction[account_sk] instead would count SCD2 versions and overstate by every
    # account whose risk band moved inside the window — see agg_merchant_daily.tmdl.
    dims["transacting_accounts"] = _one("""
        SELECT COUNT(DISTINCT a.account_id) AS n
        FROM dbo.fact_transaction f JOIN dbo.dim_account a ON f.account_sk = a.account_sk
    """)["n"]

    agg = _one("""
        SELECT SUM(attempt_count)                 AS attempts,
               SUM(approved_count)                AS approved,
               SUM(attempted_amount_gbp_minor)    AS attempted_gbp_minor,
               SUM(distinct_account_count)        AS summed_distinct_accounts
        FROM dbo.agg_merchant_daily
    """)

    return Gold(
        txn=txn, declines_by_reason=declines_by_reason, declines_by_channel=declines_by_channel,
        three_ds=three_ds, wallets=wallets, monthly=monthly, merchants=merchants,
        disputes=disputes, dims=dims, agg=agg,
    )


# -------------------------------------------------------------------------------------
# Rendering
# -------------------------------------------------------------------------------------
def _esc(s: object) -> str:
    return html.escape(str(s))


class Report:
    """Collects HTML, and checks each tile against the model as it goes.

    The checking is the reason this is a class rather than a handful of format calls. A tile names a
    measure; that name is looked up in the TMDL to get its format string, and a name that is not
    there raises. So the model cannot be renamed out from under this page, and the set of measures
    this page actually covers is a by-product of building it rather than a number someone maintains.
    """

    def __init__(self, model: tmdl.Model) -> None:
        self.model = model
        self.measures = model.measures
        self.shown: set[str] = set()

    def _format_string(self, measure: str) -> str:
        if measure not in self.measures:
            raise ValueError(
                f"no measure named {measure!r} in semantic-model/. Either it was renamed in the "
                "TMDL and this tile was not, or this tile invented it — and a tile labelled with a "
                "measure the model does not have is worse than no tile."
            )
        fmt = self.measures[measure][1].props.get("formatString")
        if not fmt:
            raise ValueError(f"measure {measure!r} has no formatString in the TMDL")
        return fmt

    def tile(self, measure: str, value: float | None, note: str = "") -> str:
        """One KPI card, standing in for one DAX measure."""
        rendered = _format(value, self._format_string(measure))
        self.shown.add(measure)
        note_html = f'<p class="note">{_esc(note)}</p>' if note else ""
        blank = " blank" if value is None else ""
        return (
            f'<div class="tile{blank}"><p class="label">{_esc(measure)}</p>'
            f'<p class="value">{_esc(rendered)}</p>{note_html}</div>'
        )

    def shows(self, *measures: str) -> None:
        """Record measures a chart or table stands in for, where no single tile carries them."""
        for m in measures:
            self._format_string(m)  # same existence check, same reason
            self.shown.add(m)

    def value(self, measure: str, value: float | None) -> str:
        """A measure's value formatted for use inside a table cell."""
        self.shown.add(measure)
        return _format(value, self._format_string(measure))


def _tiles(*cards: str) -> str:
    return f'<div class="tiles">{"".join(cards)}</div>'


def _bar_table(headers: list[str], rows: list[list[str]], bars: list[float | None]) -> str:
    """A table whose last column is a proportional bar. `bars` is one value per row, scaled to the
    largest; `None` leaves the cell empty rather than drawing a zero-width bar that reads as a
    rendering fault."""
    top = max((b for b in bars if b is not None), default=0) or 1
    head = "".join(f"<th>{_esc(h)}</th>" for h in headers)
    body = []
    for cells, bar in zip(rows, bars, strict=True):
        tds = "".join(f"<td>{_esc(c)}</td>" for c in cells)
        width = "" if bar is None else f'<span class="bar" style="width:{100 * bar / top:.1f}%">'
        end = "" if bar is None else "</span>"
        body.append(f"<tr>{tds}<td class='barcell'>{width}{end}</td></tr>")
    return f'<table><thead><tr>{head}<th class="barcol"></th></tr></thead><tbody>' \
           f'{"".join(body)}</tbody></table>'


def _columns_svg(labels: list[str], values: list[float], caption: str) -> str:
    """A column chart as inline SVG. Hand-rolled rather than charted by a library because the output
    has to render from `file://` with no network, and a chart that needs a CDN is a chart that will
    one day be a blank rectangle."""
    w, h, pad = 720, 180, 24
    top = max(values) or 1
    step = (w - 2 * pad) / max(len(values), 1)
    bars = []
    for i, (lab, v) in enumerate(zip(labels, values, strict=True)):
        bh = (h - 2 * pad) * v / top
        x = pad + i * step
        bars.append(
            f'<rect x="{x + step * 0.15:.1f}" y="{h - pad - bh:.1f}" '
            f'width="{step * 0.7:.1f}" height="{bh:.1f}" class="col"><title>'
            f'{_esc(lab)}: {v:,.0f}</title></rect>'
        )
        if i % max(len(values) // 9, 1) == 0:
            bars.append(
                f'<text x="{x + step / 2:.1f}" y="{h - 6}" class="tick">{_esc(lab[-5:])}</text>'
            )
    return (
        f'<figure><svg viewBox="0 0 {w} {h}" role="img" aria-label="{_esc(caption)}">'
        f'<line x1="{pad}" y1="{h - pad}" x2="{w - pad}" y2="{h - pad}" class="axis"/>'
        f'{"".join(bars)}</svg><figcaption>{_esc(caption)}</figcaption></figure>'
    )


def _lines_svg(
    labels: list[str], series: list[tuple[str, list[float | None]]], caption: str
) -> str:
    """One or more rate series on a shared 0–max axis. A `None` breaks the line rather than being
    drawn as zero, because a rate that is BLANK is not a rate of nothing."""
    w, h, pad = 720, 190, 30
    flat = [v for _, vs in series for v in vs if v is not None]
    top = max(flat, default=0) or 1
    step = (w - 2 * pad) / max(len(labels) - 1, 1)

    def y(v: float) -> float:
        return h - pad - (h - 2 * pad) * v / top

    out, legend = [], []
    for idx, (name, values) in enumerate(series):
        pts, run = [], []
        for i, v in enumerate(values):
            if v is None:
                if len(run) > 1:
                    pts.append(run)
                run = []
                continue
            run.append(f"{pad + i * step:.1f},{y(v):.1f}")
        if len(run) > 1:
            pts.append(run)
        for chunk in pts:
            out.append(f'<polyline points="{" ".join(chunk)}" class="line s{idx}"/>')
        legend.append(f'<span class="key s{idx}">{_esc(name)}</span>')
    for i, lab in enumerate(labels):
        if i % max(len(labels) // 9, 1) == 0:
            out.append(f'<text x="{pad + i * step:.1f}" y="{h - 8}" class="tick">'
                       f'{_esc(lab[-5:])}</text>')
    out.append(f'<text x="{pad}" y="{pad - 10}" class="tick">peak {top * 100:.1f}%</text>')
    return (
        f'<figure><svg viewBox="0 0 {w} {h}" role="img" aria-label="{_esc(caption)}">'
        f'<line x1="{pad}" y1="{h - pad}" x2="{w - pad}" y2="{h - pad}" class="axis"/>'
        f'{"".join(out)}</svg>'
        f'<figcaption>{_esc(caption)} &middot; {" ".join(legend)}</figcaption></figure>'
    )


CSS = """
:root { color-scheme: light; --ink:#16181d; --mid:#5b6270; --line:#e3e6ec; --bg:#f7f8fa;
        --accent:#2f5d8a; --accent2:#a8452f; --blank:#8a8f9a; }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--ink); font:14px/1.5 -apple-system,
       BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
main { max-width:960px; margin:0 auto; padding:0 20px 64px; }
header.top { background:#fff; border-bottom:1px solid var(--line); padding:22px 20px 16px; }
header.top div { max-width:960px; margin:0 auto; }
h1 { font-size:20px; margin:0 0 4px; letter-spacing:-.01em; }
h2 { font-size:16px; margin:36px 0 2px; }
h3 { font-size:13px; text-transform:uppercase; letter-spacing:.06em; color:var(--mid);
     margin:26px 0 10px; font-weight:600; }
p.sub { color:var(--mid); margin:0; }
nav { margin-top:12px; } nav a { color:var(--accent); text-decoration:none; margin-right:16px;
      font-weight:600; } nav a:hover { text-decoration:underline; }
.standin { background:#fff8e6; border:1px solid #e8d79a; border-radius:6px; padding:12px 14px;
           margin:22px 0 0; }
.standin strong { display:block; margin-bottom:4px; }
.standin p { margin:0; color:#5c5230; }
.tiles { display:grid; gap:10px; grid-template-columns:repeat(auto-fill,minmax(178px,1fr)); }
.tile { background:#fff; border:1px solid var(--line); border-radius:6px; padding:12px 14px; }
.tile .label { margin:0 0 6px; font-size:11px; text-transform:uppercase; letter-spacing:.05em;
               color:var(--mid); }
.tile .value { margin:0; font-size:21px; font-weight:600; letter-spacing:-.02em;
               font-variant-numeric:tabular-nums; }
.tile .note { margin:6px 0 0; font-size:11px; color:var(--mid); line-height:1.35; }
.tile.blank .value { color:var(--blank); }
table { width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line);
        border-radius:6px; font-variant-numeric:tabular-nums; }
th,td { text-align:right; padding:7px 10px; border-bottom:1px solid var(--line); }
th:first-child,td:first-child { text-align:left; }
th { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--mid);
     font-weight:600; }
tbody tr:last-child td { border-bottom:0; }
.barcol,.barcell { width:110px; }
.bar { display:block; height:9px; background:var(--accent); border-radius:2px; min-width:1px; }
figure { margin:14px 0 0; background:#fff; border:1px solid var(--line); border-radius:6px;
         padding:12px; }
figure svg { width:100%; height:auto; display:block; }
figcaption { color:var(--mid); font-size:12px; margin-top:6px; }
.axis { stroke:var(--line); stroke-width:1; }
.col { fill:var(--accent); opacity:.85; }
.line { fill:none; stroke-width:2; stroke-linejoin:round; }
.line.s0 { stroke:var(--accent); } .line.s1 { stroke:var(--accent2); }
.tick { fill:var(--mid); font-size:9px; text-anchor:middle; }
.key { font-weight:600; } .key.s0 { color:var(--accent); } .key.s1 { color:var(--accent2); }
footer { border-top:1px solid var(--line); margin-top:44px; padding-top:18px; color:var(--mid);
         font-size:12px; }
footer code { background:#fff; border:1px solid var(--line); border-radius:3px; padding:0 4px; }
footer ul { margin:6px 0 0; padding-left:18px; }
@media print { body { background:#fff; } .tile,table,figure { break-inside:avoid; } }
"""

STANDIN = """<div class="standin"><strong>This is a stand-in for the Direct Lake report, not a
report.</strong><p>Every figure below was computed by Spark SQL over the gold Delta tables in
<code>dashboard/build_dashboard.py</code> — <em>not</em> by the DAX measures in
<code>semantic-model/</code>, which have never been loaded by a DAX engine. Each tile is labelled
with the measure it stands in for, and the build fails if that measure is not in the model. The
point of the page is that the model's arithmetic produces numbers a payments analyst would
recognise; it is not evidence that the model itself loads.</p></div>"""


def _bps(num: float | None, den: float | None) -> float | None:
    r = divide(num, den)
    return None if r is None else r * 10000


def _page_exec(r: Report, g: Gold) -> str:
    t, d, dims, agg = g.txn, g.disputes, g.dims, g.agg
    attempts = t["attempts"]
    approved = t["approved"]

    by3ds = {bool(row["is_3ds"]): row for row in g.three_ds}
    with3 = divide(by3ds.get(True, {}).get("approved"), by3ds.get(True, {}).get("attempts"))
    without3 = divide(by3ds.get(False, {}).get("approved"), by3ds.get(False, {}).get("attempts"))
    uplift = None if with3 is None or without3 is None else (with3 - without3) * 100

    volume = _tiles(
        r.tile("Attempts", attempts),
        r.tile("Approved Transactions", approved),
        r.tile("Declined Transactions", t["declined"]),
        r.tile("Reversed Transactions", t["reversed"]),
        r.tile("Attempted Volume (GBP)", pounds(t["attempted_gbp_minor"])),
        r.tile("Approved Volume (GBP)", pounds(t["approved_gbp_minor"])),
        # `DIVIDE([Approved Volume (GBP)], [Approved Transactions])` — the numerator is the measure
        # above, so the conversion is already in it and must not be applied twice. The same shape as
        # the DAX, for the same reason.
        r.tile("Average Approved Value (GBP)", divide(pounds(t["approved_gbp_minor"]), approved)),
    )
    rates = _tiles(
        r.tile("Authorisation Rate", divide(approved, attempts)),
        r.tile("Decline Rate", divide(t["declined"], attempts)),
        r.tile("3DS Approval Uplift (pp)", uplift,
               "ECOM only, on both sides. Percentage points, not a relative change."),
        r.tile("Settled Transactions", t["settled"]),
        r.tile("Settlement Rate", divide(t["settled"], approved)),
        r.tile("Average Settlement Lag (Days)", t["avg_settlement_lag"]),
        r.tile("FX Carried Share", divide(t["fx_carried"], attempts)),
    )
    disputes = _tiles(
        r.tile("Disputes Raised", d["raised"]),
        r.tile("Disputes Open", d["open_now"]),
        r.tile("Disputes Won", d["won"]),
        r.tile("Disputes Lost", d["lost"]),
        r.tile("Dispute Win Rate", divide(d["won"], (d["won"] or 0) + (d["lost"] or 0))),
        r.tile("Disputed Volume (GBP)", pounds(d["disputed_gbp_minor"])),
        r.tile("Chargeback Rate (bps)", _bps(d["raised"], attempts)),
        # Both sides in pounds, as in the DAX. The 1/100 cancels and the bps figure is identical
        # either way — which is worth doing rather than skipping, because a reader comparing this
        # line to the measure should find the same expression, not a simplified one.
        r.tile("Chargeback Volume Rate (bps)",
               _bps(pounds(d["disputed_gbp_minor"]), pounds(t["attempted_gbp_minor"]))),
        r.tile("Disputes Resolved", d["resolved"],
               "Excludes open disputes. The second date role this measure uses is invisible on a "
               "page with no date filter."),
        r.tile("Dispute Resolution Days", d["resolution_days"]),
    )
    estate = _tiles(
        r.tile("Active Accounts", dims["accts"], "SCD2 current versions, counted on the "
               "business key."),
        r.tile("Active Customers", dims["custs"]),
        r.tile("Active Merchants", dims["mrchs"]),
        r.tile("Average Merchant Risk Score", dims["risk"]),
        r.tile("Distinct Accounts", dims["transacting_accounts"],
               "Accounts that transacted, through the detail fact."),
        r.tile("Distinct Accounts (per day)", None,
               "Blank by design: the measure refuses any grain coarser than one day, and this page "
               "has no date filter. The guard working, not a missing number."),
    )

    tie = "" if agg["attempts"] == attempts else (
        f' They do <strong>not</strong> tie here: {agg["attempts"]:,} against {attempts:,}. The '
        "aggregate is a full rebuild, so that means a partial run — re-run <code>make run</code>."
    )
    over = divide(agg["summed_distinct_accounts"], dims["transacting_accounts"])
    agg_tiles = _tiles(
        r.tile("Attempts (agg)", agg["attempts"]),
        r.tile("Approved (agg)", agg["approved"]),
        r.tile("Attempted Volume (agg, GBP)", pounds(agg["attempted_gbp_minor"])),
        r.tile("Authorisation Rate (agg)", divide(agg["approved"], agg["attempts"])),
    )
    agg_note = (
        f'<p class="sub">One row per merchant-day, and the additive measures tie exactly to the '
        f'detail fact above — which is what makes the aggregate safe to offer and is why the proc '
        f'stores counts rather than rates.{tie} Summing '
        f'<code>distinct_account_count</code> across every day gives '
        f'{agg["summed_distinct_accounts"]:,}, against {dims["transacting_accounts"]:,} accounts '
        f'that actually transacted — {over:.1f}&times; too high. That gap is exactly what the '
        f'grain guard on <em>Distinct Accounts (per day)</em> exists to make unobtainable.</p>'
    )

    r.shows("Attempted Volume (GBP)")
    # `year_month` is stored as an int (202401); the chart labels it as text.
    months = [str(m["ym"]) for m in g.monthly]
    volume_chart = _columns_svg(
        months, [pounds(float(m["attempted_gbp_minor"])) or 0.0 for m in g.monthly],
        "Attempted volume by month, in GBP — [Attempted Volume (GBP)] sliced by dim_date",
    )
    rate_chart = _lines_svg(
        months,
        [("Authorisation Rate", [divide(m["approved"], m["attempts"]) for m in g.monthly]),
         ("Decline Rate", [divide(m["declined"], m["attempts"]) for m in g.monthly])],
        "Authorisation and decline rate by month",
    )

    merch_rows = [
        [m["merchant_name"], m["mcc_category"], f'{m["attempts"]:,}',
         r.value("Authorisation Rate", divide(m["approved"], m["attempts"])),
         r.value("Attempted Volume (GBP)", pounds(m["attempted_gbp_minor"]))]
        for m in g.merchants
    ]
    merchants = _bar_table(
        ["Merchant", "Category", "Attempts", "Auth rate", "Attempted volume"],
        merch_rows, [pounds(float(m["attempted_gbp_minor"])) or 0.0 for m in g.merchants],
    )

    return f"""
<h2 id="exec">Page 1 &middot; Executive</h2>
<p class="sub">The card grid a Direct Lake report would open on.</p>
<h3>Volume</h3>{volume}
<h3>Rates and settlement</h3>{rates}
<h3>Disputes</h3>{disputes}
<h3>Estate</h3>{estate}
<h3>Trend</h3>{volume_chart}{rate_chart}
<h3>Top merchants by attempted volume</h3>
<p class="sub">Grouped on the merchant business key, not the surrogate key, so an SCD2 version
change does not split one merchant into two rows.</p>{merchants}
<h3>The aggregate table</h3>{agg_tiles}{agg_note}
"""


def _page_declines(r: Report, g: Gold) -> str:
    t = g.txn
    declined = t["declined"]

    heads = _tiles(
        r.tile("Declined Transactions", declined),
        r.tile("Decline Rate", divide(declined, t["attempts"])),
        r.tile("Retriable Decline Share", divide(t["retriable_declines"], declined),
               "The operationally actionable half: a technical decline can be retried, "
               "insufficient funds cannot."),
    )

    # The shares below are this page's stand-in for [Decline Rate by Reason], whose denominator is
    # every decline in the period rather than every decline left in a slicer — which is why they sum
    # to 100% here with no filter applied.
    r.shows("Decline Rate by Reason")
    reason_rows = [
        [x["reason"], x["code"], x["category"], "yes" if x["retriable"] else "no",
         f'{x["declines"]:,}', _format(divide(x["declines"], declined), _PERCENT)]
        for x in g.declines_by_reason
    ]
    reasons = _bar_table(
        ["Reason", "Code", "Category", "Retriable", "Declines", "Share of declines"],
        reason_rows, [float(x["declines"]) for x in g.declines_by_reason],
    )

    chan_rows = [
        [c["channel"], f'{c["attempts"]:,}', f'{c["declined"]:,}',
         r.value("Decline Rate", divide(c["declined"], c["attempts"]))]
        for c in g.declines_by_channel
    ]
    channels = _bar_table(
        ["Channel", "Attempts", "Declines", "Decline rate"], chan_rows,
        [divide(c["declined"], c["attempts"]) for c in g.declines_by_channel],
    )

    by3ds = {bool(row["is_3ds"]): row for row in g.three_ds}
    ds_rows, ds_bars = [], []
    for flag, label in ((True, "3DS invoked"), (False, "No 3DS")):
        row = by3ds.get(flag)
        if not row:
            continue
        rate = divide(row["approved"], row["attempts"])
        ds_rows.append([label, f'{row["attempts"]:,}', f'{row["approved"]:,}',
                        r.value("Authorisation Rate", rate)])
        ds_bars.append(rate)
    three_ds = _bar_table(["ECOM segment", "Attempts", "Approved", "Auth rate"], ds_rows, ds_bars)

    wallet_rows = [
        [w["wallet"], f'{w["attempts"]:,}', f'{w["approved"]:,}',
         r.value("Authorisation Rate", divide(w["approved"], w["attempts"]))]
        for w in g.wallets
    ]
    wallets = _bar_table(["Wallet", "Attempts", "Approved", "Auth rate"], wallet_rows,
                         [float(w["attempts"]) for w in g.wallets])

    return f"""
<h2 id="declines">Page 2 &middot; Decline analysis</h2>
<p class="sub">Where a decline rate becomes something an operations team can act on.</p>
{heads}
<h3>Declines by reason</h3>
<p class="sub">The shares sum to 100% because the denominator is every decline in the period, not
every decline left in a slicer. The &minus;1 &ldquo;not applicable&rdquo; member that every
approval points at is absent because these rows are declines only.</p>{reasons}
<h3>Declines by channel</h3>{channels}
<h3>3DS, within ECOM only</h3>
<p class="sub"><code>is_3ds</code> is false off ECOM by construction, so comparing across all
channels would put every ATM withdrawal in the &ldquo;no 3DS&rdquo; group and report a channel-mix
difference as a 3DS effect. Restricting both sides is what makes the uplift on page 1 mean
anything &mdash; and it is still a selection effect, not a causal one: merchants invoke 3DS on the
traffic they are least sure about.</p>{three_ds}
<h3>Wallet type &mdash; the schema change, visible</h3>
<p class="sub">The source column did not exist until month 10, so the earlier rows are genuinely
&ldquo;not recorded&rdquo; rather than &ldquo;no wallet&rdquo;. Silver deliberately did not
<code>COALESCE</code> that to <code>'UNKNOWN'</code>, which would have erased the difference;
labelling it at the point of display is where the distinction survives.</p>{wallets}
"""


def _footer(r: Report, g: Gold) -> str:
    total = len(r.measures)
    missing = sorted(set(r.measures) - r.shown)
    listed = "".join(f"<li>{_esc(m)}</li>" for m in missing)
    return f"""
<footer>
<p><strong>{len(r.shown)} of the {total} measures</strong> in <code>semantic-model/</code> appear on
this page. That count is computed while the page is built rather than maintained by hand, and a tile
naming a measure the model does not have fails the build.</p>
{f"<p>Not shown here:</p><ul>{listed}</ul>" if missing else ""}
<p>Read from the gold Delta tables under <code>_onelake/gold/dbo/</code>
({g.txn["attempts"]:,} transaction rows, {g.disputes["raised"]:,} disputes) by
<code>dashboard/build_dashboard.py</code>. Generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC &mdash;
this file is not committed (<code>dashboard/out/</code> is in <code>.gitignore</code>), which is why
a timestamp is acceptable here and not in the generated files that are.</p>
<p>No JavaScript, no web fonts, no CDN: it renders the same from <code>file://</code> with the
network off.</p>
</footer>"""


def render(g: Gold, model: tmdl.Model) -> str:
    r = Report(model)
    exec_page = _page_exec(r, g)
    declines_page = _page_declines(r, g)
    return f"""<!doctype html>
<html lang="en-GB"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Payments medallion — gold layer</title>
<style>{CSS}</style></head>
<body>
<header class="top"><div>
<h1>Payments medallion &middot; gold layer</h1>
<p class="sub">A static stand-in for the Direct Lake report over <code>wh_gold</code>.</p>
<nav><a href="#exec">Executive</a><a href="#declines">Decline analysis</a></nav>
</div></header>
<main>
{STANDIN}
{exec_page}
{declines_page}
{_footer(r, g)}
</main></body></html>
"""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--open", action="store_true", help="open the file in a browser when done")
    args = ap.parse_args(argv)

    out = render(read_gold(), tmdl.load())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(out)
    print(f"dashboard: wrote {OUT} ({len(out):,} bytes)")
    if args.open:
        webbrowser.open(OUT.as_uri())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
