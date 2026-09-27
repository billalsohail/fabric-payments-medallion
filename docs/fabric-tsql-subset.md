# The Fabric Warehouse T-SQL subset, and how this repo holds the line

Fabric Warehouse speaks T-SQL, and that is the problem. It is *similar enough* to SQL Server that
code written from SQL Server habits parses, runs, passes review, and then fails on the tenant — or,
worse, runs on the tenant with different semantics. `datetime2` is the perfect example: valid T-SQL
everywhere else, where it silently defaults to 7 fractional digits, and invalid on Fabric, which
requires the precision to be stated and caps it at 6.

So the constraint is not "write T-SQL". It is "write the subset", and a subset is only real if
something enforces it. `tools/fabric_tsql_lint.py` is that something: 41 rules over every `.sql`
file in `src/warehouse/`, run by `make lint` and by CI on every push.

## Why the linter carries a load nothing else can

This repo has no Fabric tenant (see the README status box), and it has no T-SQL engine either — the
SQL Server container never pulled. What it does have is `src/lib/gold.py`, which reads each statement
out of the deliverable `.sql` files, transpiles it `tsql` → `spark` with sqlglot, and executes it
against the silver Delta tables. That harness is genuine evidence and it is evidence about exactly
one thing: **the logic**. `tests/test_gold_recon.py` ties every row and every penny from silver to the
star schema.

It is evidence about **the dialect** of precisely nothing. sqlglot will happily transpile constructs
Fabric rejects, and Spark will happily run the result. A green reconciliation over a `money` column, a
`DEFAULT` constraint and an `UPDATE ... FROM` would be just as green as this one — and all three would
fail on a real Warehouse.

That is the whole reason this tool exists, and the reason the two claims are never stated together:

| Claim | Mechanism | What it cannot tell you |
|---|---|---|
| The gold logic is correct | `src/lib/gold.py` + `tests/test_gold_recon.py` | anything about Fabric |
| The gold code is portable to Fabric Warehouse | `tools/fabric_tsql_lint.py` | anything about arithmetic |

See [`gold-execution.md`](gold-execution.md) for the longer version, including what the sqlglot
translation drops and why each omission is acceptable.

## Where the rules came from

Not from memory. Every rule cites the Microsoft Learn page it was read out of, and the page's
`ms.date`, so a rule that has gone stale can be found rather than guessed at. `--rules` prints the
sources:

```
$ python tools/fabric_tsql_lint.py --rules
  surface-area   .../fabric/data-warehouse/tsql-surface-area          (ms.date 2026-08-26)
  data-types     .../fabric/data-warehouse/data-types                 (ms.date 2026-08-26)
  identity       .../fabric/data-warehouse/identity                   (ms.date 2026-09-09)
  tables         .../fabric/data-warehouse/tables                     (ms.date 2026-04-03)
  transactions   .../fabric/data-warehouse/transactions               (ms.date 2026-06-03)
  create-table   .../t-sql/statements/create-table-azure-sql-data-warehouse?view=fabric   (2025-12-29)
  rowcount       .../t-sql/functions/rowcount-transact-sql?view=fabric (ms.date 2024-06-06)
  update         .../t-sql/queries/update-transact-sql?view=fabric     (ms.date 2025-01-29)
```

Reading those pages properly was the single most useful hour of preparation behind this repo, and
four of the things it turned up contradicted what I would have written from memory. Those four are
below, because a linter that encodes my assumptions is worth nothing — the point is that it encodes
the documentation.

## The four corrections

**1. `IDENTITY` is supported.** The plan this repo was built from said to ban it. That would have been
a linter rejecting valid platform code, which is a worse failure than having no linter: it teaches you
a constraint that does not exist. What is actually unsupported is a `(seed, increment)` argument, and
the column must be `bigint` — so those are the rules (`FB107`, `FB108`), and bare
`sk bigint IDENTITY` passes. `tests/test_fabric_tsql_lint.py::test_identity_is_not_banned` exists
solely to pin this down.

This repo still does not *use* `IDENTITY`, and the distinction between "unsupported" and "declined" is
the point. Fabric's allocation is gappy and does not guarantee the order values are assigned in across
a distributed insert, so the same business key can receive a different surrogate on a re-run. For a
warehouse whose central claim is that a rerun reproduces its output, a key generator allowed to
disagree with itself is the wrong tool — and because the foreign keys are `NOT ENFORCED` (the only
form Fabric accepts), a shifted key does not fail anything. It silently repoints facts. So surrogate
keys come from `ROW_NUMBER() OVER (ORDER BY <business key>) + current MAX`, and the full argument lives
in the header of `src/warehouse/procs/03_sp_load_dim_account.sql`.

**2. `UPDATE ... FROM` does not run on Fabric Warehouse.** This is the correction that cost real work.
The standard SCD2 re-sync — `UPDATE dbo.dim SET valid_to = s.valid_to FROM stg.dim AS s WHERE ...` —
is the shape every correlated update in a dimension load naturally wants to be. Fabric supports only
single-table `UPDATE`; the `FROM` clause is not accepted. Three procs carry a note about what they did
instead:

- `03_sp_load_dim_account.sql` — a `MERGE` for the matched case plus a separate `INSERT` for the new
  one, with the reasoning written out at length in its header. More code, and it is the code that runs.
- `06_sp_load_dim_fx_rate.sql` — the constraint changed the *design*, not just the syntax. An
  incremental FX load has to reach back and close the previous batch's final interval per currency
  pair, which is `UPDATE ... FROM` shaped; expressing it as a `MERGE` would mean staging a synthetic
  close-out row per pair per batch purely to have something to match on. Reading the full history and
  computing `LEAD` over it is shorter *and* correct, so the proc truncates and rebuilds.
- `00_sp_seed_reference_data.sql` — states the repo-wide rule in its header, so the next proc author
  meets it before writing the statement.

`FB022` exists to stop the habit coming back.

**3. Recursive CTEs are not supported.** `dim_date` is the obvious victim — a date spine is the
textbook recursive CTE. It is built from a cross-joined tally instead (`FB015` catches both
`WITH RECURSIVE` and a CTE that references its own alias).

**4. `@@ROWCOUNT` is not documented as applying to Warehouse**, and `BEGIN`/`COMMIT TRANSACTION` reset
it to 0 — so a proc that logs "rows affected" from it can log 0 while having written a million rows.
`FB023` is the one rule deliberately set to **warn** rather than error, because "not documented" is
weaker evidence than "documented as unsupported" and a linter should distinguish the two. The procs
count with `COUNT_BIG` over the same predicate instead, and write the result to `stg.load_log`.

## The rules

41 rules, plus `FB000` for a file the tokenizer cannot read at all. Several codes are triggered by
more than one pattern (`FB005` matches four kinds of `CREATE INDEX`), so the number of patterns is
higher than the number of codes. Everything is an **error** except the three marked *warn*.

### FB0xx — statements and session state (`surface-area`, `tables`, `transactions`)

| Code | Rejects |
|---|---|
| `FB000` | the file could not be tokenized — fails the build rather than passing silently |
| `FB001` | `CREATE TRIGGER` |
| `FB002` | `CREATE SYNONYM` |
| `FB003` | `CREATE SEQUENCE`, `NEXT VALUE FOR` |
| `FB004` | `CREATE MATERIALIZED VIEW` |
| `FB005` | `CREATE INDEX` in all four spellings, including clustered columnstore |
| `FB006` | `CREATE TYPE` |
| `FB007` | `CREATE EXTERNAL TABLE` |
| `FB008` | `CREATE USER`, `CREATE LOGIN` — Fabric principals are Entra identities, not server logins |
| `FB009` | `SET ROWCOUNT` (use `TOP`) |
| `FB010` | `SET TRANSACTION ISOLATION LEVEL` |
| `FB011` | `SELECT ... FOR XML` |
| `FB012` | `BULK INSERT` (use `COPY INTO`) |
| `FB013` | `PREDICT` |
| `FB014` | `sp_showspaceused` |
| `FB015` | recursive CTEs — `WITH RECURSIVE`, and a CTE referencing its own alias |
| `FB016` | global `##temp` tables — only session-scoped `#temp` is supported |
| `FB017` | *warn*: a CTE nested inside a CTE (preview feature) |
| `FB018` | multi-column `CREATE STATISTICS` |
| `FB019` | *warn*: a statement sqlglot could not parse, so only token rules applied to it |
| `FB020` | `BEGIN DISTRIBUTED TRANSACTION` |
| `FB021` | `SAVE TRANSACTION` — a failed statement rolls the whole transaction back |
| `FB022` | `UPDATE ... FROM` |
| `FB023` | *warn*: `@@ROWCOUNT` |

### FB1xx — `CREATE TABLE` shape (`create-table`, `tables`, `identity`)

| Code | Rejects |
|---|---|
| `FB101` | Synapse dedicated-pool `WITH (...)` options — `DISTRIBUTION`, `HEAP`, `PARTITION`, and the rest |
| `FB102` | more than 4 `CLUSTER BY` columns |
| `FB103` | computed columns |
| `FB104` | `DEFAULT` constraints |
| `FB105` | any constraint in the column list — Fabric's `CREATE TABLE` grammar accepts none |
| `FB106` | more than 1,024 columns |
| `FB107` | `IDENTITY(seed, increment)` |
| `FB108` | a non-`bigint` `IDENTITY` column |

`FB101` has one deliberate exception worth knowing about: `PARTITION BY` inside a window function is
ordinary supported T-SQL. Only the table *option* is a problem, so the rule checks for the `PARTITION
BY` phrase rather than the bare word.

### FB2xx — column types (`data-types`, `create-table`)

| Code | Rejects |
|---|---|
| `FB201` | an unsupported type, with the replacement named (below) |
| `FB202` | `datetime2` or `time` without an explicit precision — Fabric has no default |
| `FB203` | `datetime2(n)`/`time(n)` with n > 6 |
| `FB204` | `varchar`/`char`/`varbinary` longer than 8000 (use `(MAX)`) |
| `FB205` | `decimal`/`numeric` precision above 38 |

The unsupported types, and what each becomes:

| Unsupported | Use instead |
|---|---|
| `money`, `smallmoney` | `bigint` minor units — which this repo already does everywhere, for unrelated reasons ([`data-contracts.md`](data-contracts.md)) |
| `datetime`, `smalldatetime` | `datetime2(n)`, n in 0..6 |
| `datetimeoffset` | store UTC in `datetime2(n)`, carry the offset separately |
| `nchar`, `nvarchar` | `char`, `varchar` — Fabric's are already UTF-8 |
| `text`, `ntext` | `varchar(n)` or `varchar(MAX)` |
| `image`, `binary` | `varbinary(n)` or `varbinary(MAX)` |
| `tinyint` | `smallint` |
| `json` | `varchar` plus the JSON functions |
| `geography`, `geometry` | decimal columns |
| `xml`, `sql_variant`, `hierarchyid`, `vector` | no substitute; model it explicitly |
| `rowversion`, `timestamp` | a batch id or a `datetime2` audit column (T-SQL `timestamp` is `rowversion`, not a datetime — the name is a trap) |

### FB3xx — constraints (`tables`)

| Code | Rejects |
|---|---|
| `FB301` | `PRIMARY KEY` or `UNIQUE` other than `NONCLUSTERED ... NOT ENFORCED` |
| `FB302` | `FOREIGN KEY` without `NOT ENFORCED` |
| `FB303` | `CHECK` constraints |

Constraints on Fabric are metadata for the optimiser, not enforcement. This is not a small detail
dressed up as one: it means **the DQ layer in silver is the integrity guarantee**, and a duplicate that
gets past `src/lib/dq.py` reaches the star schema with nothing in the warehouse to stop it. The
constraints in `ddl/04_constraints.sql` are declared anyway — the optimiser uses them, and they
document intent — with a header saying plainly that they enforce nothing.

### FB4xx — identifiers (`create-table`)

| Code | Rejects |
|---|---|
| `FB401` | an object name containing `/` or `\`, or ending in `.` |
| `FB402` | an identifier longer than 128 characters |

## What this tool cannot tell you

Stated plainly, because the value of a static check is bounded by how well its limits are understood.

- **It is not an engine.** It proves the code is inside the documented grammar. It cannot prove a
  query returns the right answer, performs acceptably, or that a `MERGE` will not deadlock.
- **Supported is not correct.** `FB022` will not fire on an `UPDATE` that updates the wrong rows.
- **It checks files, not a deployment.** Cross-database queries against the lakehouse SQL endpoint,
  workspace permissions, `GRANT UNMASK`, and the RLS predicate in `05_security.sql` are all
  unverifiable without a tenant, and they are where the remaining risk sits.
- **The documentation moves.** Every rule records an `ms.date`; the oldest is 2024-06-06. A rule whose
  page has changed will still be enforced, correctly or not, until someone re-reads it. That is a
  maintenance burden rather than a bug, and naming it is part of the design.
- **Two rules are warnings on purpose** (`FB017`, `FB023`). Both are cases where the documentation is
  silent or says "preview" rather than "unsupported". Promoting either to an error would be asserting
  more than the source supports.

## The tool's own tests

`tests/test_fabric_tsql_lint.py` is 70 tests, and it exists because a linter that is wrong is worse
than no linter — it manufactures confidence. Four real bugs were found by writing them, or by writing
this page:

1. **A blank line inside a `--` comment run ended comment handling**, so text after it was linted as
   code. A comment saying `-- no DEFAULT here` tripped `FB104`.
2. **`/* ... */` block comments were not handled at all.**
3. **The statement splitter dropped everything after a string literal containing a semicolon**, which
   silently removed later statements from the AST pass — the failure mode where a linter reports clean
   because it stopped reading.
4. **`CREATE LOGIN` was not caught**, while `ddl/05_security.sql` stated in a header comment that the
   linter rejected both it and `CREATE USER` — and cited the wrong rule code for good measure. This one
   was found by cross-checking every `FB` code cited anywhere in the repo against what that code
   actually matches, while writing the tables above. A documented, plausible, false claim about your own
   tool is worse than an undocumented gap, so the rule was added rather than the comment softened.

All four are now tests, which is the only reason to trust the count of zero findings the build
reports.

## Adding a rule

1. Read the Learn page. Add it to the source table if it is new, with its `ms.date`.
2. Add the pattern — token phrase for a statement-level rule, or an AST check in the `CREATE TABLE`
   pass for a column-level one.
3. Add a test that fails without the rule, and a test that a *valid* construct nearby still passes.
   The second test is the one that matters: every rule is an opportunity to ban something legal.
4. `make lint` must stay green over `src/warehouse/`. If a new rule fires on the deliverable, either
   the deliverable is wrong or the rule is — and finding out which is the entire purpose.
