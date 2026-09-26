"""Reject T-SQL that Fabric Warehouse would not accept, before a tenant ever sees it.

Gold is a Fabric Warehouse (`docs/design-decisions.md` #1), but no tenant was available to run it
against (`docs/gold-execution.md`). A local SQL Server — or a human — will happily accept
`IDENTITY(1,1)`, an enforced foreign key, a `DEFAULT` constraint and a `datetime` column, none of
which Fabric Warehouse supports. "It ran locally" is therefore evidence about the *logic* and no
evidence at all about *portability*. This linter is what carries the portability claim, and it is
the verification that works with or without a database.

## Provenance

Every rule cites the Microsoft Learn page it came from, with that page's `ms.date`, in `SOURCES`
below and in each rule's `source` field. The rule list was built by reading those pages while
writing this file, not from memory, because the surface area moves: the plan this repo was built
from asserted "there is no `IDENTITY`" and instructed that `IDENTITY` be banned outright. That was
true of earlier Fabric and is **false now** — `IDENTITY` is supported, and a linter written from
memory would have rejected valid code and been wrong in the most embarrassing possible direction.
See `docs/fabric-tsql-subset.md` for what this repo does about `IDENTITY` and why.

Re-check the pages before trusting a rule that fails on code you believe is valid. A stale rule
that blocks correct code is the failure mode to expect here.

## Why there are two passes

`sqlglot` is a transpiler, and the two ways that shapes its behaviour both defeat a naive
AST-walking linter:

1. **It falls back to an opaque `Command` node, or fails outright, on the statements that matter
   most.** `ALTER TABLE ... ADD CONSTRAINT pk PRIMARY KEY NONCLUSTERED (a) NOT ENFORCED` — the
   *only* form Fabric accepts — does not parse; it becomes `Command`. The *unsupported* enforced
   form parses cleanly into a real `ForeignKey` node. An AST-only linter is therefore blind to the
   correct spelling and sighted on the wrong one, which is worse than useless. `DISTRIBUTION =`,
   `FOR XML` and `SET TRANSACTION ISOLATION LEVEL` raise `ParseError` outright.
2. **Its type model normalises away the spelling distinctions the subset is defined in terms of.**
   `datetime`, `smalldatetime` and `datetime2` all parse to `Type.DATETIME`, and rendering that
   node back to T-SQL emits `DATETIME2` for all three — an unsupported type silently rewritten into
   a supported one. `text` and `ntext` round-trip to `VARCHAR(MAX)` the same way. `tinyint` parses
   to `Type.UTINYINT`, not `TINYINT`. So neither the enum nor the round-trip can tell an
   unsupported type from a supported one, and type rules must read the raw source spelling.

So: a **token pass** over `sqlglot.tokenize()` output carries most rules, and an **AST pass**
carries the few things structure expresses better than tokens (recursive CTEs, materialized views).
The token pass is not a regex over the file — the tokenizer strips comments and tags string
literals, so no rule here can fire on the word `DEFAULT` in a comment or inside a quoted string,
which is the specific reason a regex implementation was rejected.

## Known limitations, stated rather than discovered

- A statement `sqlglot` cannot tokenize at all is reported (`FB000`) rather than skipped silently.
- The token pass identifies a `CREATE TABLE` column list positionally. A column *named* with a bare
  unsupported type keyword (`create table t (datetime int)`, unbracketed) would be mis-read as a
  type. Bracket-quoting it (`[datetime]`) is detected and exempted.
- This checks dialect and surface area. It cannot check what an MPP engine does at runtime: a plan
  that spills, or a `MERGE` against a badly distributed table. Those are day-one-with-a-tenant
  questions and they are the first items in `docs/fabric-deployment.md`'s runbook.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# sqlglot logs a warning to stderr every time it falls back to `Command`, which for ordinary
# procedural T-SQL (`BEGIN TRY`, `END CATCH`) is most statements in this repo. Silenced so the only
# thing this tool writes is its own findings.
logging.getLogger("sqlglot").setLevel(logging.CRITICAL)

import sqlglot  # noqa: E402
from sqlglot import exp  # noqa: E402
from sqlglot.tokens import Token, TokenType  # noqa: E402

# --------------------------------------------------------------------------------------
# Provenance. Read these before changing a rule.
# --------------------------------------------------------------------------------------

SOURCES = {
    # Not a Learn page: FB019 reports a shortcoming of *this file* rather than of the SQL. Giving it
    # a citation slot anyway keeps `Finding.render` total — the alternative was a special case in
    # the renderer, and a renderer with a special case for one rule is a renderer that will grow
    # another.
    "linter": (
        "this file — sqlglot could not parse the statement; the AST pass was skipped for it",
        "n/a",
    ),
    "surface-area": (
        "https://learn.microsoft.com/fabric/data-warehouse/tsql-surface-area",
        "ms.date 2026-08-26",
    ),
    "data-types": (
        "https://learn.microsoft.com/fabric/data-warehouse/data-types",
        "ms.date 2026-08-26",
    ),
    "identity": (
        "https://learn.microsoft.com/fabric/data-warehouse/identity",
        "ms.date 2026-09-09",
    ),
    "tables": (
        "https://learn.microsoft.com/fabric/data-warehouse/tables",
        "ms.date 2026-04-03",
    ),
    "transactions": (
        "https://learn.microsoft.com/fabric/data-warehouse/transactions",
        "ms.date 2026-06-03",
    ),
    "create-table": (
        "https://learn.microsoft.com/sql/t-sql/statements/"
        "create-table-azure-sql-data-warehouse?view=fabric",
        "ms.date 2025-12-29",
    ),
}

ERROR = "error"
WARN = "warn"

# --------------------------------------------------------------------------------------
# The subset
# --------------------------------------------------------------------------------------

# Unsupported for a *persisted column*. Variables, parameters and return types may still use these
# — the data-types page restricts only table columns — which is why these are checked inside a
# `CREATE TABLE` column list and nowhere else.
#
# `binary` is here on thinner evidence than the rest: the data-types page's unsupported list does
# not name it, but the Fabric CREATE TABLE syntax block offers `varbinary` and omits `binary`
# entirely. Flagged because a silently-truncated binary column is worse than an argument about a
# doc, and `varbinary` is what this repo wants in every case anyway.
UNSUPPORTED_COLUMN_TYPES = {
    "money": "use bigint minor units (this repo already does — docs/data-contracts.md)",
    "smallmoney": "use bigint minor units",
    "datetime": "use datetime2(n), n in 0..6",
    "smalldatetime": "use datetime2(n), n in 0..6",
    "datetimeoffset": "store UTC in datetime2(n) and carry the offset separately",
    "nchar": "use char — Fabric varchar/char are already UTF-8",
    "nvarchar": "use varchar — Fabric varchar/char are already UTF-8",
    "text": "use varchar(n) or varchar(MAX)",
    "ntext": "use varchar(n) or varchar(MAX)",
    "image": "use varbinary(n) or varbinary(MAX)",
    "tinyint": "use smallint",
    "geography": "unsupported; model coordinates as decimal columns",
    "geometry": "unsupported; model coordinates as decimal columns",
    "json": "use varchar and the JSON functions",
    "xml": "unsupported",
    "sql_variant": "unsupported; use an explicit type",
    "hierarchyid": "unsupported",
    "rowversion": "unsupported; use a batch id or datetime2 audit column",
    "timestamp": "unsupported (T-SQL `timestamp` is rowversion, not a datetime)",
    "vector": "unsupported in Warehouse",
    "binary": "use varbinary(n)",
}

# Types whose precision Fabric requires explicitly: "There's no default precision like other SQL
# platforms. You must provide the value for precision from 0 to 6." A bare `datetime2` is valid
# T-SQL on SQL Server (defaulting to 7) and invalid on Fabric, which makes it exactly the kind of
# drift this tool exists to catch.
PRECISION_REQUIRED = {"datetime2": 6, "time": 6}

MAX_STRING_LENGTH = 8000
MAX_DECIMAL_PRECISION = 38
MAX_COLUMNS_PER_TABLE = 1024
MAX_IDENTIFIER_LENGTH = 128
MAX_CLUSTER_BY_COLUMNS = 4

# Table-level options that belong to Synapse dedicated SQL pools, not Fabric. They are the single
# easiest way to write gold DDL that looks right, passes review, and is rejected by the target: the
# Synapse and Fabric CREATE TABLE references live on the *same Learn page* behind a version
# selector, so copying the wrong block is a two-click mistake.
SYNAPSE_TABLE_OPTIONS = {
    "distribution": "Fabric has no distribution syntax; use WITH (CLUSTER BY (...)) if needed",
    "heap": "Fabric has no HEAP option",
    "columnstore": "Fabric has no CLUSTERED COLUMNSTORE INDEX option",
    "partition": "Fabric has no partitioned tables; use CLUSTER BY",
}

# Statement-initial keyword sequences that are simply not in the surface area. Matched on the
# token stream because most of these do not survive parsing into anything inspectable.
BANNED_SEQUENCES: list[tuple[str, tuple[str, ...], str, str, str]] = [
    ("FB001", ("create", "trigger"), "triggers are not supported", "surface-area", ERROR),
    ("FB002", ("create", "synonym"), "synonyms are not supported", "surface-area", ERROR),
    ("FB003", ("create", "sequence"),
     "sequences are not supported; IDENTITY or a deterministic key expression instead",
     "tables", ERROR),
    ("FB004", ("create", "materialized", "view"),
     "materialized views are not supported in Warehouse", "surface-area", ERROR),
    ("FB005", ("create", "index"), "user-created indexes are not supported", "tables", ERROR),
    ("FB005", ("create", "unique", "index"), "unique indexes are not supported", "tables", ERROR),
    ("FB005", ("create", "clustered", "columnstore", "index"),
     "columnstore indexes are not supported", "tables", ERROR),
    ("FB005", ("create", "nonclustered", "index"),
     "user-created indexes are not supported", "tables", ERROR),
    ("FB006", ("create", "type"), "user-defined types are not supported", "tables", ERROR),
    ("FB007", ("create", "external", "table"),
     "external tables are not supported; use OneLake shortcuts or COPY INTO", "tables", ERROR),
    ("FB008", ("create", "user"),
     "CREATE USER is not supported; Fabric uses Entra ID identities and workspace roles",
     "surface-area", ERROR),
    ("FB009", ("set", "rowcount"), "SET ROWCOUNT is not supported; use TOP", "surface-area", ERROR),
    ("FB010", ("set", "transaction", "isolation", "level"),
     "SET TRANSACTION ISOLATION LEVEL is not supported (Warehouse is snapshot isolation)",
     "surface-area", ERROR),
    ("FB012", ("bulk", "insert"), "BULK LOAD/BULK INSERT is not supported; use COPY INTO",
     "surface-area", ERROR),
    ("FB011", ("for", "xml"), "SELECT ... FOR XML is not supported", "surface-area", ERROR),
    ("FB013", ("predict",), "PREDICT is not supported in Warehouse", "surface-area", ERROR),
    ("FB014", ("sp_showspaceused",), "sp_showspaceused is not supported", "surface-area", ERROR),
    ("FB003", ("next", "value", "for"), "sequences are not supported", "tables", ERROR),
    # The transactions page lists these four under Limitations. They matter to this repo because
    # every gold load proc wraps its writes in an explicit transaction, so the one construct a
    # SQL Server habit would reach for — a named transaction, to make nested BEGIN/COMMIT pairs
    # legible — is exactly the one Fabric refuses.
    ("FB020", ("begin", "distributed", "transaction"),
     "distributed transactions are not supported", "transactions", ERROR),
    ("FB020", ("begin", "distributed", "tran"),
     "distributed transactions are not supported", "transactions", ERROR),
    ("FB021", ("save", "transaction"),
     "save points are not supported; a failed statement rolls the whole transaction back",
     "transactions", ERROR),
    ("FB021", ("save", "tran"),
     "save points are not supported; a failed statement rolls the whole transaction back",
     "transactions", ERROR),
]


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    col: int
    rule: str
    severity: str
    message: str
    source: str

    def render(self) -> str:
        url, vintage = SOURCES[self.source]
        head = f"{self.path}:{self.line}:{self.col}: {self.severity.upper()} [{self.rule}] {self.message}"
        return f"{head}\n    → {url} ({vintage})"


# --------------------------------------------------------------------------------------
# Token plumbing
#
# The tokenizer emits *compound* keyword tokens: `PRIMARY KEY`, `FOREIGN KEY` and `CLUSTER BY` each
# arrive as a single token whose text contains a space. Matching token-by-token against
# ("primary", "key") therefore silently never fires — which is how the first draft of this file
# passed an `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (id)` with no `NOT ENFORCED`, the exact
# statement the rule exists to catch. So every rule below works over a *word-level* view in which
# compound tokens are split, with each word keeping a reference back to the token it came from for
# line/column reporting.
# --------------------------------------------------------------------------------------

def _start_col(tok: Token) -> int:
    """`Token.col` is the column of the token's last character; findings want its first."""
    return max(1, tok.col - len(tok.text) + 1)


def _is_quoted(tok: Token) -> bool:
    """True for `[name]` / `"name"` / `'literal'` — never a keyword, whatever it spells."""
    return tok.token_type in (TokenType.IDENTIFIER, TokenType.STRING)


@dataclass
class Statement:
    """One semicolon-delimited statement, in both token and word form."""

    tokens: list[Token]
    words: list[str]  # lowercased, compound tokens split into their words
    anchor: list[Token]  # anchor[i] is the token that produced words[i]

    @classmethod
    def build(cls, tokens: list[Token]) -> Statement:
        words: list[str] = []
        anchor: list[Token] = []
        prev_command = False
        for tok in tokens:
            # `EXEC` is a COMMAND token and the tokenizer swallows everything after it into a single
            # STRING — so `EXEC sp_showspaceused 'dbo.t'` arrives as ['exec', <one string>]. That
            # payload is unparsed *source*, not a literal, so its words are scanned. Without this an
            # `EXEC` of any unsupported stored procedure is invisible to every rule.
            if prev_command and tok.token_type == TokenType.STRING:
                for part in tok.text.lower().replace("'", " ").split():
                    words.append(part)
                    anchor.append(tok)
                prev_command = False
                continue
            prev_command = tok.token_type == TokenType.COMMAND
            if _is_quoted(tok):
                # A quoted identifier is a name, not a keyword: `[trigger]` is a legal column and
                # `'DEFAULT'` is a legal string. Kept in the stream as a single opaque word so
                # positions still line up, but spelled so no keyword rule can match it.
                words.append("\x00quoted")
                anchor.append(tok)
                continue
            parts = tok.text.lower().split()
            for part in parts or [""]:
                words.append(part)
                anchor.append(tok)
        return cls(tokens=tokens, words=words, anchor=anchor)

    def phrase_at(self, i: int, phrase: tuple[str, ...]) -> bool:
        return tuple(self.words[i:i + len(phrase)]) == phrase

    def contains(self, phrase: tuple[str, ...]) -> bool:
        return any(self.phrase_at(i, phrase) for i in range(len(self.words)))

    def finding(self, i: int, rule: str, severity: str, message: str, source: str,
               path: str) -> Finding:
        tok = self.anchor[i]
        return Finding(path, tok.line, _start_col(tok), rule, severity, message, source)


def split_statements(tokens: list[Token]) -> list[Statement]:
    """Segment on semicolons.

    Coarse by design: a procedure body's statements become separate segments, which is what every
    rule here wants, since no rule's scope spans a semicolon. `BEGIN`/`END` nesting is therefore
    not tracked.
    """
    out: list[Statement] = []
    current: list[Token] = []
    for tok in tokens:
        if tok.token_type == TokenType.SEMICOLON:
            if current:
                out.append(Statement.build(current))
            current = []
        else:
            current.append(tok)
    if current:
        out.append(Statement.build(current))
    return out


# --------------------------------------------------------------------------------------
# Token rules
# --------------------------------------------------------------------------------------

def _banned_phrases(stmt: Statement, path: str) -> list[Finding]:
    out: list[Finding] = []
    for rule, phrase, message, source, severity in BANNED_SEQUENCES:
        for i in range(len(stmt.words)):
            if stmt.phrase_at(i, phrase):
                out.append(stmt.finding(i, rule, severity, message, source, path))
    return out


def _global_temp_tables(stmt: Statement, path: str) -> list[Finding]:
    """`##name` arrives as two separate `HASH` tokens, not one `##name` token."""
    out: list[Finding] = []
    toks = stmt.tokens
    for i in range(len(toks) - 2):
        if toks[i].text == "#" and toks[i + 1].text == "#" and toks[i + 2].text.isidentifier():
            tok = toks[i]
            out.append(Finding(
                path, tok.line, _start_col(tok), "FB016", ERROR,
                f"global temporary table ##{toks[i + 2].text} — only session-scoped #temp tables "
                "are supported",
                "tables"))
    return out


def _transaction_names(stmt: Statement, path: str) -> list[Finding]:
    """Named and marked transactions are unsupported; `BEGIN TRAN;` must be anonymous.

    Detected by looking at the *token type* of what follows `TRAN`/`TRANSACTION` rather than by
    counting words, because `split_statements` segments on semicolons only: a `BEGIN TRAN` written
    without its terminator runs into the next statement and would otherwise be reported as named
    because `INSERT` followed it. An identifier or string is a name; a statement keyword is not.
    """
    out: list[Finding] = []
    named_tokens = (TokenType.VAR, TokenType.IDENTIFIER, TokenType.STRING)
    for i in range(len(stmt.words) - 2):
        if stmt.words[i] not in ("begin", "commit", "rollback"):
            continue
        if stmt.words[i + 1] not in ("tran", "transaction"):
            continue
        verb = stmt.words[i].upper()
        if stmt.words[i + 2] == "with" and stmt.words[i + 3:i + 4] == ["mark"]:
            out.append(stmt.finding(
                i + 2, "FB021", ERROR,
                "marked transactions (WITH MARK) are not supported", "transactions", path))
        elif stmt.anchor[i + 2].token_type in named_tokens:
            out.append(stmt.finding(
                i + 2, "FB020", ERROR,
                f"named transactions are not supported; write {verb} TRAN with no name "
                f"(found {stmt.anchor[i + 2].text!r})",
                "transactions", path))
    return out


def _identifier_limits(stmt: Statement, path: str) -> list[Finding]:
    out: list[Finding] = []
    for i, tok in enumerate(stmt.tokens):
        if tok.token_type not in (TokenType.VAR, TokenType.IDENTIFIER):
            continue
        name = tok.text.strip("[]\"")
        prev = stmt.tokens[i - 1].text.lower() if i else ""
        # Only a name in a definition position is worth checking; a slash inside a string literal
        # is ordinary data.
        if prev in ("table", "schema", "view", "procedure", "proc"):
            if "/" in name or "\\" in name or name.endswith("."):
                out.append(Finding(
                    path, tok.line, _start_col(tok), "FB401", ERROR,
                    f"object name {tok.text!r} may not contain '/' or '\\' or end with '.'",
                    "create-table"))
        if len(name) > MAX_IDENTIFIER_LENGTH:
            out.append(Finding(
                path, tok.line, _start_col(tok), "FB402", ERROR,
                f"identifier is {len(name)} characters; the limit is {MAX_IDENTIFIER_LENGTH}",
                "create-table"))
    return out


def _constraint_enforcement(stmt: Statement, path: str) -> list[Finding]:
    """PK/UNIQUE need `NONCLUSTERED` and `NOT ENFORCED`; FK needs `NOT ENFORCED`.

    Checked on tokens because the *compliant* form does not parse: sqlglot renders
    `ADD CONSTRAINT pk PRIMARY KEY NONCLUSTERED (a) NOT ENFORCED` as an opaque `Command`, while the
    unsupported enforced form parses cleanly into a real `ForeignKey` node. An AST-only rule would
    be blind to correct code and sighted only on wrong code.
    """
    out: list[Finding] = []
    if not stmt.contains(("constraint",)):
        return out

    not_enforced = stmt.contains(("not", "enforced"))
    nonclustered = stmt.contains(("nonclustered",))

    for i, word in enumerate(stmt.words):
        if word == "check":
            out.append(stmt.finding(
                i, "FB303", ERROR,
                "CHECK constraints are not supported; enforce the rule in the DQ layer instead",
                "tables", path))
        elif stmt.phrase_at(i, ("primary", "key")) and (not not_enforced or not nonclustered):
            out.append(stmt.finding(
                i, "FB301", ERROR,
                "PRIMARY KEY is supported only as NONCLUSTERED ... NOT ENFORCED",
                "tables", path))
        elif stmt.phrase_at(i, ("foreign", "key")) and not not_enforced:
            out.append(stmt.finding(
                i, "FB302", ERROR,
                "FOREIGN KEY is supported only as NOT ENFORCED",
                "tables", path))
        elif word == "unique" and not stmt.contains(("index",)) and (
                not not_enforced or not nonclustered):
            out.append(stmt.finding(
                i, "FB301", ERROR,
                "UNIQUE is supported only as NONCLUSTERED ... NOT ENFORCED",
                "tables", path))
    return out


# --------------------------------------------------------------------------------------
# CREATE TABLE
# --------------------------------------------------------------------------------------

@dataclass
class _Column:
    name: Token
    type_token: Token | None = None
    type_params: list[str] = field(default_factory=list)
    trailing: list[Token] = field(default_factory=list)
    computed: bool = False


CONSTRAINT_STARTERS = {"constraint", "primary", "unique", "foreign", "check", "index"}


def _split_column_list(tokens: list[Token]) -> tuple[list[_Column], list[Token]]:
    """Split a CREATE TABLE column list into (columns, table-level constraint heads).

    Positional rather than AST-driven because the AST erases the type *spellings* the Fabric subset
    is written in terms of — `datetime`, `smalldatetime` and `datetime2` all become `Type.DATETIME`
    and all render back as `DATETIME2`. See the module docstring.

    Elements are separated by commas at paren depth 1, so a type's own parameters
    (`decimal(18, 2)`) and a constraint's column list (`(a, b)`) sit at depth 2 and cannot be
    mistaken for a column break.
    """
    try:
        open_at = next(i for i, t in enumerate(tokens) if t.token_type == TokenType.L_PAREN)
    except StopIteration:
        return [], []

    elements: list[list[Token]] = []
    current: list[Token] = []
    depth = 1
    i = open_at + 1
    while i < len(tokens):
        tok = tokens[i]
        if tok.token_type == TokenType.L_PAREN:
            depth += 1
        elif tok.token_type == TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                break
        elif tok.token_type == TokenType.COMMA and depth == 1:
            elements.append(current)
            current = []
            i += 1
            continue
        current.append(tok)
        i += 1
    if current:
        elements.append(current)

    columns: list[_Column] = []
    constraint_heads: list[Token] = []
    for element in elements:
        if not element:
            continue
        head = element[0]
        if not _is_quoted(head) and head.text.lower().split()[0] in CONSTRAINT_STARTERS:
            constraint_heads.append(head)
            continue
        col = _Column(name=head)
        rest = element[1:]
        if rest and rest[0].token_type == TokenType.ALIAS:
            # `total AS amount_minor * 2` — a computed column. `AS` occupies the position a type
            # would, so without this branch it is read as the column's type and the rule never fires.
            col.computed = True
            col.trailing = rest
            columns.append(col)
            continue
        if rest:
            col.type_token = rest[0]
            rest = rest[1:]
            # Type parameters only count when the paren *immediately* follows the type; otherwise
            # `bigint IDENTITY(1,1)` has its `(1,1)` swallowed as the type's parameters and the
            # IDENTITY rule sees a bare IDENTITY.
            if rest and rest[0].token_type == TokenType.L_PAREN:
                j, params, d = 1, [], 1
                while j < len(rest) and d:
                    if rest[j].token_type == TokenType.L_PAREN:
                        d += 1
                    elif rest[j].token_type == TokenType.R_PAREN:
                        d -= 1
                        if not d:
                            break
                    elif rest[j].token_type != TokenType.COMMA:
                        params.append(rest[j].text)
                    j += 1
                col.type_params = params
                rest = rest[j + 1:]
        col.trailing = rest
        columns.append(col)
    return columns, constraint_heads


def _multi_column_stats(stmt: Statement, path: str) -> list[Finding]:
    """`CREATE STATISTICS ... ON t (a, b)` — the surface-area page lists manually created
    multi-column stats as unsupported. Single-column stats are fine and are in fact how you give
    the Warehouse optimiser what it needs, so the column count is the whole rule.
    """
    out: list[Finding] = []
    for i, word in enumerate(stmt.words):
        if word != "statistics" or not stmt.phrase_at(i - 1, ("create", "statistics")):
            continue
        after = next((t for t in stmt.tokens[stmt.tokens.index(stmt.anchor[i]):]
                      if t.token_type == TokenType.L_PAREN), None)
        if after is not None and _count_paren_group(after, stmt.tokens) > 1:
            out.append(stmt.finding(
                i, "FB018", ERROR,
                "manually created multi-column statistics are not supported; create one "
                "single-column statistics object per column instead",
                "surface-area", path))
    return out


def _create_table_rules(stmt: Statement, path: str) -> list[Finding]:
    words = stmt.words
    if len(words) < 3 or words[0] != "create" or "table" not in words[:4]:
        return []
    out: list[Finding] = []

    columns, constraint_heads = _split_column_list(stmt.tokens)

    if len(columns) > MAX_COLUMNS_PER_TABLE:
        tok = stmt.tokens[0]
        out.append(Finding(path, tok.line, _start_col(tok), "FB106", ERROR,
                           f"{len(columns)} columns; the limit is {MAX_COLUMNS_PER_TABLE}",
                           "create-table"))

    for head in constraint_heads:
        out.append(Finding(
            path, head.line, _start_col(head), "FB105", ERROR,
            "the Fabric CREATE TABLE syntax accepts no constraints in the column list; declare it "
            "with ALTER TABLE ADD CONSTRAINT ... NOT ENFORCED",
            "create-table"))

    for col in columns:
        out.extend(_column_rules(col, path))
    out.extend(_table_options(stmt, path))
    return out


def _column_rules(col: _Column, path: str) -> list[Finding]:
    out: list[Finding] = []
    anchor = col.type_token or col.name
    trailing = [t.text.lower() for t in col.trailing]

    if col.computed:
        tok = col.trailing[0] if col.trailing else col.name
        out.append(Finding(
            path, tok.line, _start_col(tok), "FB103", ERROR,
            "computed columns are not supported; materialise the expression in the load",
            "tables"))
        return out

    if col.type_token is not None and not _is_quoted(col.type_token):
        out.extend(_type_rules(col, path))

    if "default" in trailing:
        tok = col.trailing[trailing.index("default")]
        out.append(Finding(
            path, tok.line, _start_col(tok), "FB104", ERROR,
            "DEFAULT constraints are not in the Fabric CREATE TABLE syntax; supply the value "
            "explicitly in the INSERT",
            "create-table"))

    if "identity" in trailing:
        idx = trailing.index("identity")
        tok = col.trailing[idx]
        # IDENTITY *is* supported. The plan this repo was built from said it was not, and banning it
        # would have been the wrong call — see docs/fabric-tsql-subset.md. What is unsupported is a
        # seed/increment, and the column must be bigint.
        if idx + 1 < len(col.trailing) and col.trailing[idx + 1].token_type == TokenType.L_PAREN:
            out.append(Finding(
                path, tok.line, _start_col(tok), "FB107", ERROR,
                "IDENTITY is supported but a (seed, increment) is not; use bare IDENTITY",
                "identity"))
        if col.type_token is not None and col.type_token.text.lower() != "bigint":
            out.append(Finding(
                path, anchor.line, _start_col(anchor), "FB108", ERROR,
                f"IDENTITY columns must be bigint, not {col.type_token.text}",
                "identity"))
    return out


def _type_rules(col: _Column, path: str) -> list[Finding]:
    assert col.type_token is not None
    tok = col.type_token
    name = tok.text.lower()
    params = col.type_params

    def at(rule: str, message: str, source: str) -> Finding:
        return Finding(path, tok.line, _start_col(tok), rule, ERROR, message, source)

    def first_int() -> int:
        try:
            return int(params[0])
        except (IndexError, ValueError):
            return -1

    if name in UNSUPPORTED_COLUMN_TYPES:
        return [at("FB201",
                   f"column type {name!r} is not supported for a persisted column — "
                   f"{UNSUPPORTED_COLUMN_TYPES[name]}",
                   "data-types")]
    if name in PRECISION_REQUIRED:
        cap = PRECISION_REQUIRED[name]
        if not params:
            return [at("FB202",
                       f"{name} requires an explicit precision on Fabric (0..{cap}); it has no "
                       "default, unlike SQL Server",
                       "create-table")]
        if first_int() > cap:
            return [at("FB203",
                       f"{name}({params[0]}) exceeds the Fabric maximum of {cap} fractional digits",
                       "create-table")]
    if name in ("varchar", "char", "varbinary") and params and params[0].lower() != "max":
        if first_int() > MAX_STRING_LENGTH:
            return [at("FB204",
                       f"{name}({params[0]}) exceeds {MAX_STRING_LENGTH}; use {name}(MAX)",
                       "create-table")]
    if name in ("decimal", "numeric") and params and first_int() > MAX_DECIMAL_PRECISION:
        return [at("FB205",
                   f"{name} precision {params[0]} exceeds the maximum of "
                   f"{MAX_DECIMAL_PRECISION}",
                   "create-table")]
    return []


def _table_options(stmt: Statement, path: str) -> list[Finding]:
    """Synapse-only `WITH (...)` options, and the Fabric CLUSTER BY column cap."""
    out: list[Finding] = []
    words = stmt.words
    for i, word in enumerate(words):
        if word in SYNAPSE_TABLE_OPTIONS:
            # `PARTITION BY` in a window function is ordinary supported T-SQL; only the table
            # option is a problem.
            if word == "partition" and stmt.phrase_at(i, ("partition", "by")):
                continue
            out.append(stmt.finding(
                i, "FB101", ERROR,
                f"{word.upper()} is Azure Synapse dedicated-pool syntax, not Fabric — "
                f"{SYNAPSE_TABLE_OPTIONS[word]}",
                "create-table", path))
        if stmt.phrase_at(i, ("cluster", "by")):
            cols = _count_paren_group(stmt.anchor[i], stmt.tokens)
            if cols > MAX_CLUSTER_BY_COLUMNS:
                out.append(stmt.finding(
                    i, "FB102", ERROR,
                    f"CLUSTER BY names {cols} columns; the maximum is "
                    f"{MAX_CLUSTER_BY_COLUMNS}",
                    "create-table", path))
    return out


def _count_paren_group(after: Token, tokens: list[Token]) -> int:
    """Count comma-separated items in the paren group following `after`."""
    try:
        start = tokens.index(after)
    except ValueError:
        return 0
    depth, items = 0, 0
    for tok in tokens[start:]:
        if tok.token_type == TokenType.L_PAREN:
            depth += 1
            if depth == 1:
                items = 1
        elif tok.token_type == TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                break
        elif tok.token_type == TokenType.COMMA and depth == 1:
            items += 1
    return items


# --------------------------------------------------------------------------------------
# AST rules — only where structure beats tokens
# --------------------------------------------------------------------------------------

# `MASKED WITH (FUNCTION = '...')` is valid Fabric T-SQL — it is how dynamic data masking is
# declared, and declaring it inline is the only non-preview way to do it (ALTER COLUMN is preview).
# sqlglot cannot parse it: it raises `Expecting )` on the column definition. Since the AST pass
# skips any statement that fails to parse, leaving this alone meant every AST rule was silently
# disabled on `dim_customer` — the one table in the warehouse holding PII, and so the last one
# that should have reduced coverage. Masks are stripped for the *AST pass only*; the token pass
# reads the original text, which is why the column-type and DEFAULT rules kept working throughout.
_MASK_CLAUSE = re.compile(
    r"\s+MASKED\s+WITH\s*\(\s*FUNCTION\s*=\s*'(?:[^']|'')*'\s*\)", re.IGNORECASE
)

# `CREATE SCHEMA` crashes sqlglot's TSQL dialect outright (an `AttributeError` inside
# `_parse_create`, not a `ParseError`). No AST rule in this file has anything to say about a
# `CREATE SCHEMA` anyway, so it is skipped deliberately and by name. The alternative — letting it
# fall into the generic unparseable branch below — would emit a warning on every deployment script
# that creates a schema, which trains the reader to ignore that warning.
_AST_EXEMPT = re.compile(r"^\s*CREATE\s+SCHEMA\b", re.IGNORECASE)


def _ast_rules(sql: str, path: str) -> list[Finding]:
    """Parse statement-by-statement, so one exotic statement cannot blind the whole pass.

    `sqlglot.parse` over a whole file raises on the first construct it cannot handle — and several
    constructs this linter rejects (`WITH (DISTRIBUTION = ...)`, `FOR XML`) are exactly that. Parsing
    the file as one unit therefore returned nothing at all for files that contained any violation,
    which silently disabled every AST rule on precisely the files that needed them.

    A statement that still fails to parse is **reported** (FB019) rather than skipped in silence.
    That is the difference between "the AST rules found nothing here" and "the AST rules never ran
    here", and conflating the two is how a linter comes to certify code it never inspected — which
    matters more than usual in this repo, where the linter is the *only* verification the warehouse
    SQL gets (see docs/gold-execution.md).
    """
    out: list[Finding] = []
    for text, line in _sql_statements(sql):
        if _AST_EXEMPT.match(text):
            continue
        try:
            tree = sqlglot.parse_one(_MASK_CLAUSE.sub("", text), dialect="tsql")
        except Exception as exc:
            out.append(Finding(
                path, line, 1, "FB019", WARN,
                f"statement could not be parsed, so no AST rule was applied to it "
                f"({type(exc).__name__}: {str(exc).splitlines()[0][:80]}); "
                "token-based rules still ran",
                "linter"))
            continue
        if tree is None:
            continue
        for with_node in tree.find_all(exp.With):
            if with_node.args.get("recursive"):
                out.append(Finding(
                    path, line, 1, "FB015", ERROR,
                    "WITH RECURSIVE is not supported; build the sequence from a cross-joined tally",
                    "surface-area"))
            for cte in with_node.expressions:
                alias = cte.alias_or_name
                if alias and any(t.name == alias for t in cte.this.find_all(exp.Table)):
                    out.append(Finding(
                        path, line, 1, "FB015", ERROR,
                        f"CTE {alias!r} references itself; recursive queries are not supported — "
                        "build the sequence from a cross-joined tally instead",
                        "surface-area"))
        for with_node in tree.find_all(exp.With):
            # Sequential CTEs (`WITH a AS (...), b AS (SELECT FROM a)`) are GA. A CTE whose *body*
            # declares its own `WITH` is a *nested* CTE, which the surface-area page calls a preview
            # feature. Preview is not unsupported, so this is a warning, not an error — but a gold
            # layer that depends on a preview feature is a gold layer that can regress without a
            # code change, which is worth knowing before deployment rather than after.
            for cte in with_node.expressions:
                if any(inner is not with_node for inner in cte.this.find_all(exp.With)):
                    out.append(Finding(
                        path, line, 1, "FB017", WARN,
                        f"CTE {cte.alias_or_name!r} contains a nested CTE, which is a preview "
                        "feature; flatten it into a sequential CTE chain instead",
                        "surface-area"))

        for create in tree.find_all(exp.Create):
            props = create.args.get("properties")
            if props and any(isinstance(p, exp.MaterializedProperty) for p in props.expressions):
                out.append(Finding(
                    path, line, 1, "FB004", ERROR,
                    "materialized views are not supported in Warehouse", "surface-area"))
    return out


def _blank_line_comments(sql: str) -> str:
    """Replace the body of every `--` comment with spaces, preserving line and column positions.

    This must happen before the `;` split, and the reason is a bug FB019 exposed rather than one
    that was foreseen. The DDL in this repo is more prose than SQL, and English prose contains
    semicolons — so a naive split shredded every commented file into fragments like
    "...only in `04_constraints.sql`" and handed them to sqlglot, which quite reasonably failed on
    all of them. The AST pass was therefore running on almost none of the warehouse DDL while
    reporting a clean bill of health, which is the single worst thing a linter can do.

    Columns are preserved rather than the comment simply being dropped, because every `Finding`
    carries a position a reader is expected to be able to jump to.

    A `--` inside a string literal is left alone by tracking quote parity along the line. That is
    not a full lexer, and it does not need to be: the token pass already segments on real
    `SEMICOLON` tokens from sqlglot's own tokenizer, so this function's only job is to stop prose
    from masquerading as SQL.
    """
    out: list[str] = []
    for line in sql.splitlines():
        in_string = False
        cut = None
        i = 0
        while i < len(line):
            ch = line[i]
            if ch == "'":
                in_string = not in_string
            elif ch == "-" and not in_string and line[i:i + 2] == "--":
                cut = i
                break
            i += 1
        out.append(line if cut is None else line[:cut] + " " * (len(line) - cut))
    return "\n".join(out)


def _sql_statements(sql: str) -> list[tuple[str, int]]:
    """Comment-stripped `;` split, each fragment paired with the line its first non-blank character
    is on.

    Good enough for the AST pass, which cares about query shape: a `;` inside a string literal would
    at worst yield two fragments that fail to parse and are reported as FB019. The token pass, which
    does the precise work, segments on real `SEMICOLON` tokens instead.
    """
    out: list[tuple[str, int]] = []
    line = 1
    for chunk in _blank_line_comments(sql).split(";"):
        if chunk.strip():
            blank = len(chunk) - len(chunk.lstrip("\n\r \t"))
            out.append((chunk, line + chunk[:blank].count("\n")))
        line += chunk.count("\n")
    return out


# --------------------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------------------

TOKEN_RULES = (
    _banned_phrases,
    _global_temp_tables,
    _transaction_names,
    _identifier_limits,
    _constraint_enforcement,
    _create_table_rules,
    _multi_column_stats,
)


def lint_sql(sql: str, path: str) -> list[Finding]:
    try:
        tokens = sqlglot.tokenize(sql, dialect="tsql")
    except Exception as exc:
        return [Finding(path, 1, 1, "FB000", ERROR, f"could not tokenize: {exc}", "surface-area")]

    findings: list[Finding] = []
    for stmt in split_statements(tokens):
        for rule in TOKEN_RULES:
            findings.extend(rule(stmt, path))
    findings.extend(_ast_rules(sql, path))
    return sorted(set(findings), key=lambda f: (f.line, f.col, f.rule))


def collect_files(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            out.extend(sorted(p.rglob("*.sql")))
        elif p.suffix == ".sql":
            out.append(p)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Reject T-SQL outside the Fabric Warehouse surface area.")
    ap.add_argument("paths", nargs="*", help=".sql files or directories to lint")
    ap.add_argument("--json", action="store_true", help="emit findings as JSON")
    ap.add_argument("--warnings-as-errors", action="store_true")
    ap.add_argument("--rules", action="store_true", help="print the rule sources and exit")
    args = ap.parse_args(argv)

    if args.rules:
        print("Fabric Warehouse T-SQL subset — rule sources")
        for key, (url, vintage) in SOURCES.items():
            print(f"  {key:14} {url}  ({vintage})")
        return 0

    if not args.paths:
        ap.error("at least one path is required")

    files = collect_files(args.paths)
    if not files:
        print(f"fabric-tsql-lint: no .sql files under {', '.join(args.paths)}")
        return 0

    findings: list[Finding] = []
    for f in files:
        findings.extend(lint_sql(f.read_text(), str(f)))

    if args.json:
        print(json.dumps([f.__dict__ for f in findings], indent=2))
    else:
        for f in findings:
            print(f.render())

    errors = [f for f in findings if f.severity == ERROR]
    warnings = [f for f in findings if f.severity == WARN]
    verdict = "FAIL" if errors or (warnings and args.warnings_as_errors) else "OK"
    print(
        f"\nfabric-tsql-lint: {verdict} — {len(files)} file(s), "
        f"{len(errors)} error(s), {len(warnings)} warning(s)"
    )
    return 1 if verdict == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
