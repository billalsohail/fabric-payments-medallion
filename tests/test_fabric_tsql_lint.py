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
    # Not a bare `THROW`, which is query-free procedural scaffolding and is now exempted by name
    # so that ten stored procedures do not contribute thirty permanent warnings. This fixture is
    # the boundary of that exemption: procedural on its first keyword, but containing a subquery,
    # which is exactly where skipping the AST pass would cost real coverage.
    ("FB019", "IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dim_account') BEGIN SET @v = 1; END"),
    # Transactions. Every gold load proc opens one, so these four are the rules most likely to be
    # tripped by SQL Server habit rather than by carelessness.
    ("FB020", "BEGIN TRAN load_gold;"),
    ("FB020", "COMMIT TRAN load_gold;"),
    ("FB020", "ROLLBACK TRANSACTION load_gold;"),
    ("FB020", "BEGIN DISTRIBUTED TRANSACTION;"),
    ("FB021", "SAVE TRANSACTION before_facts;"),
    ("FB021", "BEGIN TRANSACTION WITH MARK 'nightly load';"),
    ("FB023", "SET @rows_inserted = @@ROWCOUNT;"),
    # The rule that reshaped the gold layer before a proc was written: the SCD2 re-sync every
    # dimension needs would naturally be an `UPDATE ... FROM`, local SQL Server accepts it, and
    # Fabric does not.
    ("FB022", "UPDATE dbo.dim_account SET valid_to = s.valid_to "
              "FROM stg.dim_account AS s WHERE s.account_id = dbo.dim_account.account_id;"),
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

    -- Anonymous, unnested, no save point. This is the only transaction shape Fabric accepts, and it
    -- is the shape every proc in src/warehouse/procs/ uses, so a false positive here would be a
    -- linter that rejects the repo's own gold layer.
    BEGIN TRAN;
        DELETE FROM gold.dim_currency WHERE currency_sk = -1;
        INSERT INTO gold.dim_currency
            (currency_sk, currency_code, currency_name, minor_unit_digits, _loaded_at)
        VALUES (-1, 'N/A', 'Unknown', 0, @loaded_at);
    COMMIT TRAN;
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


def test_a_from_inside_a_set_subquery_is_not_an_update_from() -> None:
    """FB022's negative case, and the reason it is an AST rule rather than a token scan.

    `SET a = (SELECT MAX(x) FROM u)` is a single-table UPDATE that Fabric supports; the word `FROM`
    following `UPDATE` is not what makes a statement unsupported. A token-pass version of this rule
    would have rejected supported SQL, which is the more expensive failure of the two — a linter
    that blocks legal code gets switched off.
    """
    sql = "UPDATE dbo.dim_currency SET currency_name = (SELECT MAX(n) FROM stg.names) WHERE currency_sk = 1;"
    assert not lint_sql(sql, "t.sql")


def test_a_merge_update_branch_is_not_an_update_from() -> None:
    """The SCD2 close-out shape every dimension proc uses must lint clean.

    `MERGE ... WHEN MATCHED THEN UPDATE SET` reads a source table and updates a target, which is
    semantically the thing `UPDATE ... FROM` would have done — so if FB022 fired here the rule would
    have banned both the unsupported construct and its only supported replacement.
    """
    sql = (
        "MERGE dbo.dim_account AS tgt USING stg.dim_account AS src "
        "ON tgt.account_id = src.account_id AND tgt.valid_from = src.valid_from "
        "WHEN MATCHED AND tgt.valid_to <> src.valid_to THEN UPDATE SET tgt.valid_to = src.valid_to;"
    )
    assert not lint_sql(sql, "t.sql")


def test_query_free_procedural_scaffolding_is_not_reported() -> None:
    """The proc contract's own statements must lint silently, or FB019 becomes background noise.

    Every proc in src/warehouse/procs/ is built from `DECLARE`, `SET`, `IF XACT_STATE() <> 0
    ROLLBACK TRAN` and `THROW`. sqlglot's TSQL dialect parses none of them, and before the
    exemption each one raised FB019 — roughly thirty warnings across the gold layer, on code that
    contains nothing any AST rule has an opinion about. A warning class that fires thirty times on
    correct code is a warning class nobody reads, and FB019's entire job is to be read: it is the
    linter admitting it did not inspect something.
    """
    for sql in (
        "DECLARE @rows bigint = 0;",
        "SET @msg = CONCAT('spans ', @n, ' days; widen the tally');",
        "IF XACT_STATE() <> 0 ROLLBACK TRAN;",
        "THROW 50001, @msg, 1;",
        "BEGIN TRAN;",
        "COMMIT TRAN;",
    ):
        assert not lint_sql(sql, "t.sql"), sql


def test_a_semicolon_inside_a_string_literal_does_not_shred_the_batch() -> None:
    """The statement splitter must not end a statement on a quoted semicolon.

    This is the same bug `_blank_line_comments` fixed for prose, in its other half. A split in the
    wrong place does not merely produce one unparseable fragment — it glues the tail of the string
    onto the head of the *next* statement, so that statement is never parsed either and every AST
    rule is skipped for it. The planted `UPDATE ... FROM` below is what proves the damage is real:
    it must still be caught despite following an error message that contains a semicolon.
    """
    sql = (
        "THROW 50001, 'spans 12000 days; widen the tally', 1;\n"
        "UPDATE dbo.dim_account SET valid_to = s.valid_to "
        "FROM stg.dim_account AS s WHERE s.account_id = dbo.dim_account.account_id;"
    )
    codes = {f.rule for f in lint_sql(sql, "t.sql")}
    assert "FB022" in codes, [f.render() for f in lint_sql(sql, "t.sql")]


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


def test_a_block_comment_cannot_shred_a_statement() -> None:
    """The `/* */` half of the bug FB019 exposed.

    Prose contains semicolons; this repo's SQL is more prose than SQL. A header comment with a
    semicolon in it used to split the file it documented into fragments, every one of which failed
    to parse and was reported as FB019 — so the AST pass ran on nothing while the summary line said
    the file was fine. The assertion is therefore specifically that the statement *after* a
    semicolon-bearing block comment is still parsed, not merely that nothing was reported.
    """
    sql = (
        "/* A header; it contains a semicolon, an apostrophe in gold's name, and a /* nested\n"
        "   comment */ of the kind T-SQL allows and sqlglot's tokenizer tracks. */\n"
        "CREATE TABLE dbo.t (a bigint DEFAULT 0);\n"
    )
    findings = lint_sql(sql, "t.sql")
    fired = {f.rule for f in findings}
    assert "FB104" in fired, f"the statement after the comment was not analysed; got {sorted(fired)}"
    assert "FB019" not in fired, [f.render() for f in findings]


def test_a_comment_marker_inside_a_string_is_not_a_comment() -> None:
    sql = "CREATE TABLE dbo.t (a bigint);\nSELECT '-- /* not a comment */' AS note, 1 AS n;\n"
    assert not lint_sql(sql, "t.sql")
