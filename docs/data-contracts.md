# Data contracts — source feeds

Seven source feeds land in OneLake (`Files/landing/<entity>/ingest_date=YYYY-MM-DD/`). This document
is the frozen contract: the generator produces exactly these shapes, `meta_source_config` describes
how each is loaded, and `tests/test_contracts.py` fails if the generator drifts from this page.

Conventions:

- **Money is stored in minor units** (`*_minor`, integer pence/cents). No floats in money columns,
  anywhere in the stack. Currency is always carried alongside.
- **Timestamps are UTC ISO-8601**; dates are `YYYY-MM-DD`.
- Columns prefixed `_` are **source-system control columns**, not business data.
- Enum domains listed here are enforced by `enum_domain` DQ rules in silver.

## Load semantics

| Entity | Format | Arrival | Load type | Merge keys | Silver treatment |
|---|---|---|---|---|---|
| `transactions` | JSONL | daily, high volume | incremental by watermark | `transaction_id` | dedupe → DQ → append/merge |
| `accounts` | CSV | daily CDC extract | incremental by watermark | `account_id` | **SCD2** |
| `customers` | CSV | **monthly** full snapshot | full snapshot | `customer_id` | **SCD2** |
| `merchants` | Parquet | **monthly** full snapshot | full snapshot | `merchant_id` | **SCD2** |
| `disputes` | JSONL | **late-arriving, 0–90d after txn** | incremental, 90-day rolling window | `dispute_id` | merge in window |
| `fx_rates` | CSV | daily reference | incremental by watermark | `rate_date, from_currency` | merge |
| `card_products` | CSV | static, rarely changes | full snapshot | `card_product_code` | overwrite (SCD1) |

`dim_decline_reason` is a conformed lookup seeded in warehouse DDL, not a landing feed — it is
reference data owned by the warehouse, not by a source system.

**Why master data lands monthly while `accounts` lands daily.** `accounts` is a CDC extract: only
changed rows appear, so daily arrival is cheap and it is the primary SCD2 driver (`risk_band` moves,
and `_op = D` has to close a row rather than delete it). `customers` and `merchants` are *full*
snapshots, and a daily full snapshot of every customer for 18 months would be ~137M rows of almost
entirely unchanged data — neither realistic for master data nor laptop-sized. Monthly snapshots
still give SCD2 genuine history to close out. The distinction is deliberate and worth stating: the
arrival cadence a source can support is what determines whether you can afford full snapshots.

---

## `transactions` — JSONL, one object per line

Grain: **one authorisation attempt**. Not one payment — a declined attempt followed by a successful
retry is two rows, which is exactly why authorisation rate is measurable.

| Column | Type | Null | Notes |
|---|---|---|---|
| `transaction_id` | string (uuid4) | no | Business key |
| `account_id` | string | no | → `accounts` |
| `card_id` | string | no | Multiple cards per account |
| `merchant_id` | string | **yes (0.5%)** | → `merchants`. Injected nulls: ATM withdrawals legitimately have none, so the DQ rule is conditional on `channel <> 'ATM'` |
| `card_product_code` | string | no | → `card_products` |
| `amount_minor` | long | no | Minor units. Injected negatives must be quarantined |
| `currency_code` | string(3) | no | ISO 4217. Injected invalids (`XYZ`, `ZZ1`) must be quarantined |
| `auth_ts` | timestamp | no | Watermark column |
| `capture_ts` | timestamp | yes | Null unless status ≥ CAPTURED |
| `settlement_date` | date | yes | Null until settled; drives settlement-lag measure |
| `status` | string | no | `AUTHORISED` \| `DECLINED` \| `REVERSED` \| `CAPTURED` \| `SETTLED` |
| `decline_reason_code` | string | yes | Non-null **iff** `status = 'DECLINED'` — a cross-field DQ rule |
| `mcc` | string(4) | no | Merchant category code |
| `channel` | string | no | `POS` \| `ECOM` \| `ATM` \| `MOTO` |
| `country_code` | string(2) | no | ISO 3166-1 alpha-2 |
| `is_3ds` | boolean | no | Only meaningful for `ECOM`; drives the 3DS uplift measure |
| `device_id` | string | yes | Present for `ECOM` only |
| `wallet_type` | string | yes | **Appears at month 10 only** — schema evolution case. `APPLE_PAY` \| `GOOGLE_PAY` \| `NONE` |

## `accounts` — CSV, CDC extract

Emits only changed rows per day, with an operation flag. This is the SCD2 driver.

| Column | Type | Null | Notes |
|---|---|---|---|
| `account_id` | string | no | Business key |
| `customer_id` | string | no | → `customers` |
| `sort_code` | string(6) | no | |
| `account_number_masked` | string | no | Already masked at source (`****1234`) |
| `account_type` | string | no | `CURRENT` \| `SAVINGS` \| `CREDIT` |
| `status` | string | no | `ACTIVE` \| `DORMANT` \| `FROZEN` \| `CLOSED` |
| `risk_band` | string(1) | no | `A`–`E`. **Changes over time — the reason SCD2 exists here** |
| `credit_limit_minor` | long | yes | Null for non-credit accounts |
| `opened_date` | date | no | |
| `closed_date` | date | yes | Non-null iff `status = 'CLOSED'` |
| `region` | string | no | `LONDON` \| `SOUTH` \| `MIDLANDS` \| `NORTH` \| `SCOTLAND` \| `WALES` \| `NI`. RLS predicate column |
| `_op` | string(1) | no | `I` \| `U` \| `D`. `D` closes the SCD2 row, never hard-deletes |
| `_change_ts` | timestamp | no | Watermark + dedupe ordering column |

**Two things the contract deliberately does not promise about this feed.**

`status` is an attribute, not a state machine. The generator produces `CLOSED → ACTIVE` transitions
(106 of them at `tiny`), because nothing here models account lifecycle as a set of legal edges. Real
CDC feeds do produce reopenings, and a dimension is the wrong place to reject one: silver's job is to
record what the source said, not to decide the source was wrong. If the business rule existed, it
would be a DQ rule with `warn` severity and a row in `meta_dq_results` — not a silent drop. Note that
`closed_date` **is** contracted against `status` (non-null iff `CLOSED`), and that holds in both
directions with zero violations; the pair is consistent, the sequence is not constrained.

Two changes for the same `account_id` can carry the **same `_change_ts`** (71 rows at `tiny`) — a
source applying two updates inside the same minute, which is ordinary for a batch extract. A
half-open interval chain cannot represent two states at one instant without a zero-width version, and
a zero-width version is unreachable by any point-in-time join, so `src/lib/scd2.py` collapses them
deterministically (highest hash wins) rather than emitting a row no query can ever return. The
consequence is stated plainly in `tests/test_scd2.py`: the dimension preserves every *observable*
state, which is fewer than every state in the feed.

## `customers` — CSV, monthly full snapshot

Contains PII. Masking is applied in the **gold** layer (`05_security.sql`), not silver: silver keeps
the unmasked value so that reprocessing is possible, and access is controlled at the serving layer.

| Column | Type | Null | Notes |
|---|---|---|---|
| `customer_id` | string | no | Business key |
| `first_name`, `last_name` | string | no | PII |
| `email` | string | no | PII — masked in gold |
| `dob` | date | no | PII |
| `kyc_status` | string | no | `VERIFIED` \| `PENDING` \| `FAILED` |
| `country_code` | string(2) | no | |
| `segment` | string | no | `RETAIL` \| `PREMIER` \| `BUSINESS` |
| `marketing_opt_in` | boolean | no | |
| `_snapshot_date` | date | no | Snapshot identity; dedupe ordering column |

## `merchants` — Parquet, monthly full snapshot

Parquet deliberately: proves the bronze notebook is format-agnostic via config, not code branches.

| Column | Type | Null | Notes |
|---|---|---|---|
| `merchant_id` | string | no | Business key |
| `merchant_name` | string | no | |
| `mcc` | string(4) | no | |
| `category` | string | no | Derived from MCC at source |
| `country_code` | string(2) | no | |
| `acquirer_id` | string | no | |
| `risk_score` | int | no | 0–100. **Changes over time → SCD2** |
| `status` | string | no | `ACTIVE` \| `SUSPENDED` \| `TERMINATED` |
| `onboarded_date` | date | no | |
| `_snapshot_date` | date | no | |

## `disputes` — JSONL, late-arriving

Raised 0–90 days after the transaction. This is the feed that makes a naive
"process today's partition" design produce wrong numbers.

| Column | Type | Null | Notes |
|---|---|---|---|
| `dispute_id` | string | no | Business key |
| `transaction_id` | string | no | → `transactions`. **Referential DQ rule** |
| `raised_date` | date | no | 0–90 days after the transaction's `auth_ts` |
| `reason_code` | string | no | `FRAUD` \| `GOODS_NOT_RECEIVED` \| `DUPLICATE` \| `UNRECOGNISED` \| `QUALITY` |
| `disputed_amount_minor` | long | no | ≤ transaction amount |
| `currency_code` | string(3) | no | |
| `status` | string | no | `OPEN` \| `WON` \| `LOST` \| `WITHDRAWN` |
| `resolved_date` | date | yes | Non-null iff status ≠ `OPEN` |
| `_event_ts` | timestamp | no | Watermark column |

## `fx_rates` — CSV, daily reference

| Column | Type | Null | Notes |
|---|---|---|---|
| `rate_date` | date | no | |
| `from_currency` | string(3) | no | |
| `to_currency` | string(3) | no | Always `GBP` in this feed |
| `rate` | decimal(18,8) | no | Multiply to convert *from* → GBP |
| `source` | string | no | Provider name |

**Injected gaps:** a handful of dates have no rates. GBP-normalised volume must therefore
forward-fill from the last known rate and flag it, not silently drop the transaction. The
`continuity` DQ rule catches the gap and the gold load applies the fill.

`continuity` rather than `freshness`, and the distinction matters: `freshness` asks whether the
newest rate has fallen behind, which catches a feed that *stopped*. These gaps are interior — the
feed kept producing and simply skipped days. Both rules are configured on this feed because they
detect different outages, and both are batch-level: the defect is a row that does not exist, and a
missing row cannot be quarantined.

## `card_products` — CSV, static

| Column | Type | Null | Notes |
|---|---|---|---|
| `card_product_code` | string | no | Business key |
| `product_name` | string | no | |
| `network` | string | no | `VISA` \| `MASTERCARD` \| `AMEX` |
| `tier` | string | no | `CLASSIC` \| `GOLD` \| `PLATINUM` \| `BLACK` |
| `annual_fee_minor` | long | no | |
| `active_from` | date | no | |
| `active_to` | date | yes | |

---

## Injected defects — the contract for "dirt"

Deterministic under a fixed seed. Each exists to make a specific mechanism demonstrable rather than
merely asserted, and each has a matching test.

Rates, not absolute counts: the same figure has to hold at `tiny` and at `demo`, so tests assert
"defects exist and every one of them is quarantined", never a magic number. The rates live in
`DEFECTS` in `nb_00_generate_landing_data.py` and this table is the contract they answer to.

| Defect | Rate | Mechanism it proves | Test |
|---|---|---|---|
| Exact duplicate transactions | 0.3% | Dedupe on merge keys | `test_dedupe` |
| Null `merchant_id` on non-ATM | 0.5% | Conditional `not_null` rule | `test_dq_gate` |
| Negative `amount_minor` | 0.02% | `range` rule → quarantine | `test_dq_gate` |
| Invalid `currency_code` | 0.015% | `enum_domain` rule → quarantine | `test_dq_gate` |
| `decline_reason_code` inconsistent with `status` | 0.05% | Cross-field rule (both directions) | `test_dq_gate` |
| FX rate date gaps | 5 dates, evenly spread | `continuity` rule (batch-level) + forward-fill | `test_fx_gap_fill` |
| Disputes 0–90 days late | all disputes | Rolling-window reprocessing | `test_late_arriving` |
| `accounts` CDC `U`/`D` operations | 0–5 changes/account; 2% of changes are `D` | SCD2 close-out, incl. logical delete | `test_scd2_invariants` |
| `wallet_type` appears at month 10 | one-off | Schema evolution in bronze | `test_schema_evolution` |

The `wallet_type` drift is **real, not simulated**: transactions are written in two passes split at
the month-10 boundary, so files before that date genuinely lack the column. JSON carries schema per
file, so bronze has to absorb it rather than being handed a null-filled column.

## Scale

`--scale` parameter on the generator:

| Scale | Transactions | Span | Customers | Accounts | Merchants | Use |
|---|---|---|---|---|---|---|
| `tiny` | 50k | 120 days | 2k | 3k | 500 | CI; full suite must run in minutes |
| `demo` | 2M | 18 months | 50k | 80k | 8k | The committed demo dataset; laptop-sized |

`tiny` spans 120 days, not 30, even though it is the CI profile. Row count is what costs CI time,
not span — and a 30-day span produces only *one* monthly master-data snapshot (leaving SCD2 nothing
to close out) and truncates the 90-day dispute lag to ~26 days (leaving late arrivals never actually
late). A CI profile that cannot exercise the two hardest mechanisms in the pipeline is not a CI
profile worth having.

Sized for a laptop, not for realism. At 100× the first thing to break is small-file pressure in
bronze, then shuffle on the silver merges, then the decision not to partition `fact_transaction` —
in that order. See `docs/design-decisions.md` #10.
