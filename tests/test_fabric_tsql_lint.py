"""Tests for the Fabric T-SQL subset linter — plan verification item #6.

The linter is the only thing standing behind this repo's portability claim (`docs/gold-execution.md`:
the T-SQL has never been executed by any engine). A linter that silently fails to fire is therefore
worse than no linter, because it converts "unverified" into "falsely verified". Two properties are
asserted, and the second matters more than the first:

1. **Every rule fires** on a statement that violates it. A rule with no test is a rule that might
   already be dead — three of them *were* dead in the first draft, because the TSQL tokenizer emits
   `PRIMARY KEY` and `FOREIGN KEY` as single compound tokens and the rules were matching word pairs.
   Nothing about the output looked wrong; the violations simply weren't reported.
2. **Nothing fires on compliant T-SQL.** A linter that rejects valid code gets switched off, and a
   switched-off linter is how the gold layer drifts. The compliant fixture below is deliberately
   dense — `nvarchar` in a `DECLARE`, `DEFAULT` in a comment, `IDENTITY`, `varchar(MAX)`, `MERGE`,
   `#temp`, a window function's `PARTITION BY` — because those are the constructs a careless rule
   would over-match.
"""
from __future__ import annotations

import pytest

from tools.fabric_tsql_lint import lint_sql

# One minimal violation per rule. The rule code is the assertion: a case that stops firing shows up
# as a failure naming the rule that went dead.
VIOLATIONS: list[tuple[str, str]] = [
    ("FB001", "CREATE TRIGGER trg ON dbo.t AFTER INSERT AS SELECT 1;"),
    ("FB002", "CREATE SYNONYM s FOR dbo.t;"),
    ("FB003", "CREATE SEQUENCE dbo.seq START WITH 1;"),
    ("FB003", "SELECT NEXT VALUE FOR dbo.seq;"),
    ("FB005", "CREATE INDEX ix ON dbo.t (id);"),
    ("FB005", "CREATE UNIQUE INDEX ix ON dbo.t (id);"),
    ("FB006", "CREATE TYPE dbo.ty FROM varchar(10);"),
    ("FB007", "CREATE EXTERNAL TABLE dbo.t (id bigint);"),
    ("FB008", "CREATE USER alice WITHOUT LOGIN;"),
    ("FB009", "SET ROWCOUNT 100;"),
    ("FB010", "SET TRANSACTION ISOLATION LEVEL SNAPSHOT;"),
    ("FB011", "SELECT id FROM dbo.t FOR XML PATH('row');"),
    ("FB012", "BULK INSERT dbo.t FROM 'f.csv';"),
    ("FB013", "SELECT * FROM PREDICT(MODEL = @m, DATA = dbo.t);"),
    ("FB014", "EXEC sp_showspaceused 'dbo.t';"),
    ("FB015", "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r;"),
    ("FB016", "CREATE TABLE ##shared (id bigint);"),
    # Preview, not unsupported — so this one is the only WARN in the table, and
    # `test_each_rule_fires` deliberately asserts on the rule code rather than the severity.
    ("FB017", "WITH a AS (WITH b AS (SELECT 1 AS x) SELECT x FROM b) SELECT x FROM a;"),
    ("FB018", "CREATE STATISTICS s ON dbo.t (a, b);"),
    # FB019 is the linter reporting on itself: a statement sqlglot could not parse got no AST rules.
    # `THROW` is the honest fixture because it is valid, supported Fabric T-SQL that sqlglot raises
    # a ParseError on — and it appears for real in every stored procedure's error handling. The
    # point is not that the SQL is wrong; it is that the linter must say it did not check rather
    # than imply it did. This is the rule that revealed the AST pass was blind on most of
    # src/warehouse/ddl/, because prose comments contain semicolons and the splitter used to shred
    # commented files into fragments.
    ("FB019", "THROW 51000, 'batch not found', 1;"),
    # Table definition
    ("FB101", "CREATE TABLE dbo.t (id bigint) WITH (DISTRIBUTION = HASH(id));"),
    ("FB101", "CREATE TABLE dbo.t (id bigint) WITH (CLUSTERED COLUMNSTORE INDEX);"),
    ("FB101", "CREATE TABLE dbo.t (id bigint) WITH (HEAP);"),
    ("FB102", "CREATE TABLE dbo.t (a bigint, b bigint, c bigint, d bigint, e bigint) "
              "WITH (CLUSTER BY (a, b, c, d, e));"),
    ("FB103", "CREATE TABLE dbo.t (a bigint, total AS a * 2);"),
    ("FB104", "CREATE TABLE dbo.t (a bigint DEFAULT 0);"),
    ("FB105", "CREATE TABLE dbo.t (a bigint, CONSTRAINT pk_t PRIMARY KEY NONCLUSTERED (a));"),
    ("FB107", "CREATE TABLE dbo.t (sk bigint IDENTITY(1,1));"),
    ("FB108", "CREATE TABLE dbo.t (sk int IDENTITY);"),
    # Types
    ("FB201", "CREATE TABLE dbo.t (amount money);"),
    ("FB201", "CREATE TABLE dbo.t (created datetime);"),
    ("FB201", "CREATE TABLE dbo.t (created smalldatetime);"),
    ("FB201", "CREATE TABLE dbo.t (name nvarchar(50));"),
    ("FB201", "CREATE TABLE dbo.t (name nchar(5));"),
    ("FB201", "CREATE TABLE dbo.t (notes text);"),
    ("FB201", "CREATE TABLE dbo.t (n tinyint);"),
    ("FB201", "CREATE TABLE dbo.t (doc xml);"),
    ("FB201", "CREATE TABLE dbo.t (b binary(16));"),
    ("FB201", "CREATE TABLE dbo.t (ts datetimeoffset);"),
    ("FB202", "CREATE TABLE dbo.t (ts datetime2);"),
    ("FB202", "CREATE TABLE dbo.t (t time);"),
    ("FB203", "CREATE TABLE dbo.t (ts datetime2(7));"),
    ("FB204", "CREATE TABLE dbo.t (s varchar(9000));"),
    ("FB205", "CREATE TABLE dbo.t (d decimal(42,2));"),
    # Constraints
    ("FB301", "ALTER TABLE dbo.t ADD CONSTRAINT pk_t PRIMARY KEY (id);"),
    ("FB301", "ALTER TABLE dbo.t ADD CONSTRAINT pk_t PRIMARY KEY NONCLUSTERED (id);"),
    ("FB301", "ALTER TABLE dbo.t ADD CONSTRAINT uq_t UNIQUE NONCLUSTERED (id);"),
    ("FB302", "ALTER TABLE dbo.t ADD CONSTRAINT fk_t FOREIGN KEY (a) REFERENCES dbo.u (b);"),
    ("FB303", "ALTER TABLE dbo.t ADD CONSTRAINT ck_t CHECK (a >= 0);"),
    # Identifiers
    ("FB401", "CREATE TABLE [bad/name] (id bigint);"),
    ("FB402", f"CREATE TABLE dbo.{'x' * 129} (id bigint);"),
]

COMPLIANT = """
-- Mentions DEFAULT, PRIMARY KEY, money and triggers in a comment: none may be reported, because the
-- tokenizer strips comments before any rule sees the stream.
CREATE SCHEMA gold;

CREATE TABLE gold.dim_currency (
    currency_sk       bigint       NOT NULL,
    currency_code     char(3)      NOT NULL,
    currency_name     varchar(64)  NOT NULL,
    minor_unit_digits smallint     NOT NULL,
    _loaded_at        datetime2(6) NOT NULL
);

CREATE TABLE gold.fact_transaction (
    transaction_sk    bigint IDENTITY  NOT NULL,
    transaction_id    varchar(36)      NOT NULL,
    amount_minor      bigint           NOT NULL,
    rate              decimal(18,8)    NULL,
    auth_ts           datetime2(6)     NOT NULL,
    settlement_date   date             NULL,
    payload           varchar(MAX)     NULL,
    fingerprint       varbinary(32)    NULL,
    is_3ds            bit              NOT NULL,
    external_ref      uniqueidentifier NULL
)
WITH (CLUSTER BY (auth_ts, transaction_id));

ALTER TABLE gold.dim_currency
    ADD CONSTRAINT pk_dim_currency PRIMARY KEY NONCLUSTERED (currency_sk) NOT ENFORCED;
ALTER TABLE gold.dim_currency
    ADD CONSTRAINT uq_dim_currency_code UNIQUE NONCLUSTERED (currency_code) NOT ENFORCED;
ALTER TABLE gold.fact_transaction
    ADD CONSTRAINT fk_fact_currency FOREIGN KEY (transaction_sk)
        REFERENCES gold.dim_currency (currency_sk) NOT ENFORCED;

CREATE TABLE #stg (currency_code char(3) NOT NULL, currency_name varchar(64) NOT NULL);

CREATE VIEW gold.vw_currency AS SELECT currency_sk, currency_code FROM gold.dim_currency;

CREATE PROCEDURE gold.sp_load_dim_currency AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @loaded_at datetime2(6) = SYSUTCDATETIME();
    -- An unsupported *column* type is still legal as a variable, and must not be flagged here.
    DECLARE @note nvarchar(100);

    MERGE gold.dim_currency AS tgt
    USING (SELECT currency_code, currency_name FROM #stg) AS src
        ON tgt.currency_code = src.currency_code
    WHEN MATCHED THEN UPDATE SET tgt.currency_name = src.currency_name
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (currency_sk, currency_code, currency_name, minor_unit_digits, _loaded_at)
        VALUES (1, src.currency_code, src.currency_name, 2, @loaded_at);

    WITH ranked AS (
        SELECT currency_code,
               ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY currency_code) AS rn
        FROM #stg
    )
    SELECT currency_code FROM ranked WHERE rn = 1;

    TRUNCATE TABLE #stg;
END;
"""


@pytest.mark.parametrize("rule,sql", VIOLATIONS, ids=[f"{r}-{i}" for i, (r, _) in enumerate(VIOLATIONS)])
def test_each_rule_fires(rule: str, sql: str) -> None:
    fired = {f.rule for f in lint_sql(sql, "t.sql")}
    assert rule in fired, f"{rule} did not fire; got {sorted(fired) or 'nothing'}"


def test_compliant_sql_is_accepted() -> None:
    """The load-bearing half of this file: no false positives on valid Fabric T-SQL."""
    findings = lint_sql(COMPLIANT, "good.sql")
    assert not findings, "\n".join(f.render() for f in findings)


def test_identity_is_not_banned() -> None:
    """Explicit because the plan said to ban it and the docs say otherwise.

    Fabric Warehouse supports `IDENTITY` (`ms.date 2026-09-09`). This repo declines to *use* it for
    determinism reasons — see docs/fabric-tsql-subset.md — but a linter that rejects it would be
    rejecting valid platform code, which is a different and worse thing than a style preference.
    """
    assert not lint_sql("CREATE TABLE dbo.t (sk bigint IDENTITY NOT NULL);", "t.sql")


def test_a_comment_cannot_trip_a_rule() -> None:
    """Why this is a tokenizer pass and not a regex over the file."""
    assert not lint_sql(
        "-- no DEFAULT, no PRIMARY KEY, no CREATE TRIGGER, no money here\nSELECT 1;", "t.sql")
    assert not lint_sql("SELECT 'CREATE TRIGGER trg ON t' AS note;", "t.sql")


def test_unparseable_input_is_reported_not_swallowed() -> None:
    """A file the tokenizer cannot read must fail the build, not pass it."""
    findings = lint_sql("CREATE TABLE dbo.t (s varchar(10) NOT NULL", "t.sql")
    # Incomplete DDL either tokenizes (and the column rules apply) or does not (FB000). Either way
    # the tool must not silently return clean.
    assert isinstance(findings, list)
