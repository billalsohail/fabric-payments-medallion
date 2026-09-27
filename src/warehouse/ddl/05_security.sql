-- =====================================================================================
-- wh_gold — 05: security (RLS + masking grants)
--
-- ┌─────────────────────────────────────────────────────────────────────────────────────┐
-- │ UNVALIDATED, and more so than anything else in src/warehouse/.                      │
-- │                                                                                     │
-- │ No file under src/warehouse/ has been executed by any engine (docs/gold-execution   │
-- │ .md explains why). This one is additionally unverifiable *in principle* without a   │
-- │ tenant: RLS and masking are evaluated against Entra identities, and there is no      │
-- │ local substitute for a signed-in user. The logic files can at least be proven by     │
-- │ src/lib/gold.py running the same transformations in Spark SQL; a filter predicate    │
-- │ has no such fallback. Treat this file as a design, reviewable as code.               │
-- └─────────────────────────────────────────────────────────────────────────────────────┘
--
-- ## The cost of this file, stated up front
--
-- Enabling RLS on the star **gives up Direct Lake**. The Learn page on Warehouse row-level
-- security says it plainly: Power BI queries on a warehouse in Direct Lake mode fall back to
-- DirectQuery in order to honour row-level security. That is in direct tension with
-- docs/design-decisions.md #3, which chose Direct Lake for exactly the reasons DirectQuery
-- undoes — no refresh window, no second copy, vertipaq-speed scans straight off Delta.
--
-- Both things cannot be had on the same tables, so the decision has to be made rather than
-- discovered:
--
--   * **Regulated, per-region reporting** → RLS on, DirectQuery, slower interactive queries,
--     and capacity consumed per visual rather than per refresh.
--   * **One trusted analyst audience** → RLS off, Direct Lake, and access controlled by *who
--     can open the workspace* instead of by row.
--
-- The middle path, and the one this repo would actually take: **RLS on the detail star,
-- Direct Lake preserved on `agg_merchant_daily`**, which carries no region column and therefore
-- nothing to filter. The executive report reads the aggregate in Direct Lake; the regional
-- drill-down reads the detail in DirectQuery. That is a real design consequence of a security
-- requirement, and it is the kind of thing worth finding in the docs before a demo rather than
-- in a capacity bill afterwards.
--
-- ## What is *not* here, deliberately
--
-- `CREATE USER` / `CREATE LOGIN`. Fabric does not work that way: principals are Entra
-- identities granted access to the workspace or item, and they appear to T-SQL without being
-- created by it. A script that tried to create them would fail, and tools/fabric_tsql_lint.py
-- rejects both (FB008). This is one of the sharper differences from SQL Server and a fair thing
-- to be asked about.
--
-- Deployment order: last. Every object referenced here must already exist, and `WITH
-- SCHEMABINDING` makes that a hard dependency rather than a soft one — a schemabound function
-- cannot be created over a missing table, and once created it blocks ALTERs to the columns it
-- reads. That rigidity is the point: it means a later change to `dim_account.region` cannot
-- silently disable the filter.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Region entitlement mapping
-- -------------------------------------------------------------------------------------
-- Entitlements live in a table, not in the predicate function. The alternative — a function
-- full of `WHEN USER_NAME() = 'someone@bank.example' THEN ...` — needs a schema change and a
-- deployment to onboard an analyst, and puts a list of named individuals into source control.
--
-- '*' in `region` means all regions. That is a deliberate substitute for `IS_ROLEMEMBER` /
-- database-role membership: role support in Fabric Warehouse is not something this repo has
-- verified, and a security control resting on an unverified platform assumption is worse than
-- one resting on a row it owns.
--
-- Note the type: `varchar(20)`, matching `dbo.dim_account.region` exactly. A mismatch here
-- would put an implicit conversion inside a filter predicate evaluated on every row of every
-- query — the single most expensive place in the warehouse to put one.
CREATE TABLE sec.user_region_access (
    principal_name  varchar(320)    NOT NULL,
    region          varchar(20)     NOT NULL,
    granted_ts      datetime2(6)    NOT NULL
);

ALTER TABLE sec.user_region_access
    ADD CONSTRAINT pk_user_region_access
        PRIMARY KEY NONCLUSTERED (principal_name, region) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- The filter predicate
-- -------------------------------------------------------------------------------------
-- An inline table-valued function: one `RETURN (SELECT ...)`, no `BEGIN`/`END`, no procedural
-- body. A multi-statement TVF would be both unsupported here and, in a filter predicate,
-- catastrophic for performance — the predicate is evaluated per row of every query touching the
-- table, so it must be inlinable into the plan.
--
-- It returns a row when access is allowed and no rows when it is not; that is the whole
-- protocol. `WITH SCHEMABINDING` is required for a security-predicate function and is what ties
-- its lifetime to `sec.user_region_access`.
--
-- `USER_NAME()` rather than `SUSER_SNAME()` or `ORIGINAL_LOGIN()`: in Fabric this resolves to
-- the Entra principal, which is the identity the workspace actually grants to.
CREATE FUNCTION sec.fn_region_filter (@region varchar(20))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN (
    SELECT 1 AS fn_region_filter_result
    WHERE EXISTS (
        SELECT 1
        FROM sec.user_region_access AS ura
        WHERE ura.principal_name = USER_NAME()
          AND (ura.region = @region OR ura.region = '*')
    )
);

-- -------------------------------------------------------------------------------------
-- The security policy
-- -------------------------------------------------------------------------------------
-- `dim_account` is the only table carrying `region`, so it is the only table the predicate can
-- be applied to directly. The facts are filtered *transitively*: a user who cannot see a
-- London account cannot resolve its `account_sk`, so the join drops the fact.
--
-- **That transitive filtering is weaker than it sounds, and the weakness is worth naming.** An
-- unfiltered `SELECT SUM(amount_minor) FROM fact_transaction` with no join to `dim_account`
-- returns the global total to every user. Transitive protection through a star only holds for
-- queries that actually traverse the dimension. Closing it properly means denormalising
-- `region` onto `fact_transaction` and adding a second filter predicate — a real cost (a
-- redundant column on the largest table, and a value that must be re-resolved if an account
-- moves region) paid for a real gain.
--
-- This repo takes the second option — see the extra predicates on both facts below — because a security control that protects the
-- report but not the endpoint is the kind of thing that reads as finished and is not. The
-- `region` column on the fact is populated by sp_load_fact_transaction from the *then-current*
-- account version, so it inherits the same point-in-time semantics as every other fact
-- attribute.
-- One policy, three predicates. A single policy rather than three is deliberate: `STATE = OFF`
-- then disables the whole control atomically, which is what an incident actually needs. Three
-- separate policies would have to be disabled one at a time, leaving windows in which the star is
-- inconsistently filtered.
--
-- `agg_merchant_daily` is deliberately absent. It carries no region column, so there is nothing
-- to filter — and that absence is what preserves Direct Lake for the executive report, per the
-- note at the top of this file.
CREATE SECURITY POLICY sec.account_region_policy
    ADD FILTER PREDICATE sec.fn_region_filter(region) ON dbo.dim_account,
    ADD FILTER PREDICATE sec.fn_region_filter(account_region) ON dbo.fact_transaction,
    ADD FILTER PREDICATE sec.fn_region_filter(account_region) ON dbo.fact_dispute
    WITH (STATE = ON);

-- -------------------------------------------------------------------------------------
-- Masking grants — a template, not executable as written
-- -------------------------------------------------------------------------------------
-- The masks themselves are declared inline in 02_dimensions.sql (`MASKED WITH (FUNCTION = ...)`)
-- rather than applied here with `ALTER TABLE ... ALTER COLUMN`, because ALTER COLUMN is a
-- preview feature in Fabric Warehouse as of the surface-area page's ms.date of 2026-08-26, and
-- a security control that depends on a preview feature can regress without a code change.
-- Note that docs/data-contracts.md still describes masking as living in this file; that line
-- predates the ALTER COLUMN finding.
--
-- What belongs here is the *grant* side, and it cannot be written against real principals in a
-- public repo — so it is left as a commented template. This is the one place in the repo where
-- a commented-out statement is the correct artefact rather than a loose end:
--
--   GRANT UNMASK ON dbo.dim_customer TO [fraud-analysts@bank.example];
--   GRANT SELECT  ON SCHEMA::dbo    TO [regional-reporting@bank.example];
--
-- Scope UNMASK at the table, never the database. A database-scoped UNMASK is indistinguishable
-- from no masking at all, and is the most common way this feature is accidentally disabled.
--
-- ## Masking is presentation, not protection
--
-- The Learn page on dynamic data masking says so directly, and it is the most important thing
-- to understand about the feature: a user with SELECT on a masked column can recover the value
-- by probing it — `WHERE date_of_birth BETWEEN ... AND ...` returns rows or does not, and a few
-- dozen queries narrow a masked date to a day. Masking stops a value appearing on a screen; it
-- does not stop a determined user with query access from inferring it.
--
-- The consequence for this repo's architecture is concrete and worth stating: **silver is the
-- layer that must actually be access-controlled**, because silver holds the unmasked PII with
-- no masking at all (deliberately — reprocessing needs the real value). Gold's masks are a
-- convenience for the majority of analysts who have no business seeing PII and no motive to
-- probe for it. Presenting them as the PII control would be overstating what they do.
--
-- `dbo.dim_customer.birth_year` exists for exactly this reason: age banding is a legitimate
-- analytical need, and projecting the year unmasked satisfies it without granting UNMASK on
-- `date_of_birth` to everyone who wants an age histogram. Minimising who needs the exemption
-- is more durable than auditing who has it.
