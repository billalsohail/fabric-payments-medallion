-- =====================================================================================
-- wh_gold — 02: dimensions
--
-- Conventions held throughout, each one a Fabric Warehouse constraint rather than a style
-- preference. tools/fabric_tsql_lint.py enforces all of them.
--
--   * varchar, never nvarchar/nchar/text — the n-types are unsupported. The data is ASCII
--     business codes and Latin-1 names; if it were not, this would be a real problem and
--     the answer would be varchar with a UTF-8 collation, not nvarchar.
--   * datetime2(6), never datetime/smalldatetime/datetimeoffset. The precision is not
--     optional: Fabric requires exactly 6 where SQL Server defaults to 7.
--   * money in bigint minor units. money/smallmoney are unsupported, and would be the
--     wrong choice anyway — see docs/data-contracts.md.
--   * smallint, never tinyint. Unsupported, and it costs one byte to comply.
--   * no DEFAULT, no CHECK, no computed columns. All three are unsupported, which means
--     every value in these tables is set explicitly by a load proc. That is more verbose
--     and it is also the honest shape of a warehouse where the engine guarantees nothing:
--     a DEFAULT you cannot declare is a DEFAULT you cannot forget was doing work.
--   * constraints live in 04_constraints.sql, never inline, because the only form Fabric
--     accepts is ALTER TABLE ... ADD CONSTRAINT ... NOT ENFORCED.
--
-- Surrogate keys are bigint and are assigned deterministically by the load procs — see the
-- header of procs/sp_load_dim_account.sql for why not IDENTITY, which *is* supported.
--
-- Every dimension carries an **unknown member** at surrogate key -1. This is load-bearing
-- rather than decorative: with no enforced foreign keys, a fact row whose merchant is
-- absent from the dimension would join to NULL, and a NULL surrogate key silently drops
-- that row out of every measure sliced by merchant. Directing it to -1 instead means the
-- row count reconciles exactly and the loss is visible as an 'Unknown' bar on the report.
-- The DQ layer is what stops unknowns from being routine; the -1 row is what stops an
-- unknown from being invisible.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- dim_date — conformed, generated, not sourced
-- -------------------------------------------------------------------------------------
-- date_sk is a *smart* key (yyyymmdd as int), not a meaningless sequence. This is the one
-- dimension where that is the better choice: it is stable across a full rebuild, it makes
-- partition-elimination predicates on the fact readable, and it lets a loader derive the
-- key arithmetically instead of joining to look it up. Every other dimension gets an
-- opaque key, because a smart key over business data leaks that data into the fact.
CREATE TABLE dbo.dim_date (
    date_sk             int             NOT NULL,
    full_date           date            NOT NULL,
    day_of_month        smallint        NOT NULL,
    day_of_week         smallint        NOT NULL,
    day_name            varchar(10)     NOT NULL,
    day_of_year         smallint        NOT NULL,
    iso_week            smallint        NOT NULL,
    month_number        smallint        NOT NULL,
    month_name          varchar(10)     NOT NULL,
    month_start_date    date            NOT NULL,
    month_end_date      date            NOT NULL,
    quarter_number      smallint        NOT NULL,
    year_number         smallint        NOT NULL,
    year_month          int             NOT NULL,
    is_weekend          bit             NOT NULL,
    is_month_end        bit             NOT NULL
);

-- -------------------------------------------------------------------------------------
-- dim_currency — conformed reference data owned by the warehouse
-- -------------------------------------------------------------------------------------
-- minor_unit_digits is here because 'amount_minor' means different things per currency:
-- JPY has zero minor units, so 1000 JPY-minor is 1000 yen, not 10. A report that divides
-- every minor amount by 100 is wrong for JPY and nobody notices until a Japanese merchant
-- appears. The column exists so the semantic model can get it right.
CREATE TABLE dbo.dim_currency (
    currency_sk             bigint          NOT NULL,
    currency_code           varchar(3)      NOT NULL,
    currency_name           varchar(50)     NOT NULL,
    minor_unit_digits       smallint        NOT NULL,
    is_reporting_currency   bit             NOT NULL
);

-- -------------------------------------------------------------------------------------
-- dim_decline_reason — conformed reference data owned by the warehouse
-- -------------------------------------------------------------------------------------
-- Not a landing feed: no source system sends this, and the categorisation below is an
-- analytical judgement the warehouse makes. is_retriable is the point of the table —
-- 'decline rate' is not one number, because an INSUFFICIENT_FUNDS decline that succeeds on
-- retry and a SUSPECTED_FRAUD decline that must not be retried are different events with
-- the same status.
CREATE TABLE dbo.dim_decline_reason (
    decline_reason_sk       bigint          NOT NULL,
    decline_reason_code     varchar(30)     NOT NULL,
    decline_reason_desc     varchar(100)    NOT NULL,
    decline_category        varchar(20)     NOT NULL,
    is_retriable            bit             NOT NULL
);

-- -------------------------------------------------------------------------------------
-- dim_card_product — SCD1
-- -------------------------------------------------------------------------------------
-- Type 1 because the source is static reference data: docs/data-contracts.md contracts it
-- as rarely-changing, and a tier rename is a correction, not history worth keeping.
CREATE TABLE dbo.dim_card_product (
    card_product_sk     bigint          NOT NULL,
    card_product_code   varchar(10)     NOT NULL,
    product_name        varchar(50)     NOT NULL,
    network             varchar(20)     NOT NULL,
    tier                varchar(20)     NOT NULL,
    annual_fee_minor    bigint          NOT NULL,
    active_from         date            NOT NULL,
    active_to           date            NULL,
    load_batch_id       varchar(100)    NOT NULL,
    loaded_ts           datetime2(6)    NOT NULL
);

-- -------------------------------------------------------------------------------------
-- dim_account — SCD2
-- -------------------------------------------------------------------------------------
-- valid_to on the open version is the sentinel 9999-12-31, not NULL. A NULL end date turns
-- every point-in-time join into `(valid_to IS NULL OR ts < valid_to)`, which is both slower
-- and the sort of predicate someone eventually writes half of. The sentinel keeps the
-- interval half-open and total: exactly one version satisfies
-- `valid_from <= ts AND ts < valid_to` at any instant.
CREATE TABLE dbo.dim_account (
    account_sk              bigint          NOT NULL,
    account_id              varchar(20)     NOT NULL,
    customer_id             varchar(20)     NOT NULL,
    sort_code               varchar(6)      NOT NULL,
    account_number_masked   varchar(20)     NOT NULL,
    account_type            varchar(20)     NOT NULL,
    account_status          varchar(20)     NOT NULL,
    risk_band               varchar(1)      NOT NULL,
    credit_limit_minor      bigint          NULL,
    region                  varchar(20)     NOT NULL,
    opened_date             date            NOT NULL,
    closed_date             date            NULL,
    valid_from              datetime2(6)    NOT NULL,
    valid_to                datetime2(6)    NOT NULL,
    is_current              bit             NOT NULL,
    scd_hash                bigint          NOT NULL,
    load_batch_id           varchar(100)    NOT NULL,
    loaded_ts               datetime2(6)    NOT NULL
);

-- -------------------------------------------------------------------------------------
-- dim_customer — SCD2, and the only table here holding PII
-- -------------------------------------------------------------------------------------
-- Masks are declared **inline at CREATE TABLE**, not applied afterwards with
-- ALTER TABLE ... ALTER COLUMN ... ADD MASKED WITH. Both forms are documented, but
-- ALTER COLUMN is a preview feature in Fabric Warehouse as of ms.date 2026-08-26, and a
-- security control that depends on a preview feature is a security control that can regress
-- without anyone changing the code. Declaring the mask with the column also makes it
-- impossible to create the table and forget the second script.
--   https://learn.microsoft.com/fabric/data-warehouse/dynamic-data-masking (ms.date 2026-06-25)
--   https://learn.microsoft.com/fabric/data-warehouse/tsql-surface-area (ms.date 2026-08-26)
--
-- The consequence, stated here rather than discovered later: masking is *presentation*, not
-- protection. A user with SELECT can infer a masked value by range-probing it in a WHERE
-- clause — the Learn page above says so explicitly. Silver keeps the unmasked values and is
-- the layer that must actually be access-controlled; this mask stops accidental exposure in
-- a report, and nothing more. Claiming otherwise would be the worst kind of security theatre.
CREATE TABLE dbo.dim_customer (
    customer_sk         bigint          NOT NULL,
    customer_id         varchar(20)     NOT NULL,
    first_name          varchar(100)    MASKED WITH (FUNCTION = 'partial(1,"XXXXX",0)') NULL,
    last_name           varchar(100)    MASKED WITH (FUNCTION = 'partial(1,"XXXXX",0)') NULL,
    email               varchar(320)    MASKED WITH (FUNCTION = 'email()') NULL,
    date_of_birth       date            MASKED WITH (FUNCTION = 'default()') NULL,
    birth_year          smallint        NULL,
    kyc_status          varchar(20)     NOT NULL,
    country_code        varchar(2)      NOT NULL,
    segment             varchar(20)     NOT NULL,
    marketing_opt_in    bit             NOT NULL,
    valid_from          datetime2(6)    NOT NULL,
    valid_to            datetime2(6)    NOT NULL,
    is_current          bit             NOT NULL,
    scd_hash            bigint          NOT NULL,
    load_batch_id       varchar(100)    NOT NULL,
    loaded_ts           datetime2(6)    NOT NULL
);

-- birth_year is carried alongside the masked date_of_birth on purpose. Age banding is a
-- legitimate analytical need and masking date_of_birth to 1900-01-01 destroys it, so the
-- analytically-useful, individually-unidentifying part is projected into its own unmasked
-- column rather than handing out UNMASK to everyone who wants an age histogram.

-- -------------------------------------------------------------------------------------
-- dim_merchant — SCD2
-- -------------------------------------------------------------------------------------
-- risk_score is the reason this is Type 2 rather than Type 1: a merchant whose score moved
-- from 20 to 90 in June must not make last January's transactions look high-risk.
CREATE TABLE dbo.dim_merchant (
    merchant_sk         bigint          NOT NULL,
    merchant_id         varchar(20)     NOT NULL,
    merchant_name       varchar(100)    NOT NULL,
    mcc                 varchar(4)      NOT NULL,
    mcc_category        varchar(50)     NOT NULL,
    country_code        varchar(2)      NOT NULL,
    acquirer_id         varchar(20)     NOT NULL,
    risk_score          int             NOT NULL,
    risk_tier           varchar(10)     NOT NULL,
    merchant_status     varchar(20)     NOT NULL,
    onboarded_date      date            NOT NULL,
    valid_from          datetime2(6)    NOT NULL,
    valid_to            datetime2(6)    NOT NULL,
    is_current          bit             NOT NULL,
    scd_hash            bigint          NOT NULL,
    load_batch_id       varchar(100)    NOT NULL,
    loaded_ts           datetime2(6)    NOT NULL
);

-- -------------------------------------------------------------------------------------
-- dim_fx_rate — sparse source feed turned into effective-dated intervals
-- -------------------------------------------------------------------------------------
-- Not a conventional dimension, and it is here rather than in stg because the fact join
-- needs it and the semantic model does not.
--
-- docs/data-contracts.md contracts the FX feed as having interior gaps — dates where the
-- provider simply produced nothing — and requires that GBP-normalised volume forward-fill
-- from the last known rate and *flag* that it did, rather than dropping the transaction.
-- The obvious implementation of "forward fill" is a windowed LAST_VALUE ... IGNORE NULLS
-- over a dense calendar. This does the same job by turning each rate into the interval it
-- governs (`valid_from_date` to the next rate's date), which is the same half-open interval
-- idea SCD2 uses one file up. A gap then stops being a missing row that must be filled and
-- becomes an interval that is simply wider than a day — so the fill needs no special case,
-- and `is_carried` on the fact falls out of comparing the transaction date to
-- valid_from_date.
CREATE TABLE dbo.dim_fx_rate (
    fx_rate_sk          bigint          NOT NULL,
    from_currency       varchar(3)      NOT NULL,
    to_currency         varchar(3)      NOT NULL,
    rate                decimal(18,8)   NOT NULL,
    rate_date           date            NOT NULL,
    valid_from_date     date            NOT NULL,
    valid_to_date       date            NOT NULL,
    rate_source         varchar(30)     NOT NULL,
    load_batch_id       varchar(100)    NOT NULL,
    loaded_ts           datetime2(6)    NOT NULL
);
