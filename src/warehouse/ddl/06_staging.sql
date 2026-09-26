-- =====================================================================================
-- 06_staging.sql — the `stg` schema: silver, as the Warehouse sees it
--
-- Every stored procedure in `../procs/` reads from `stg.*` and never from silver directly. That
-- indirection is the single seam between the two substrates, and it is deliberately the *only* one:
--
--   On Fabric    `stg` is filled by cross-database query against the lakehouse SQL analytics
--                endpoint — `INSERT INTO stg.x SELECT ... FROM lh_silver.dbo.x`. The Warehouse
--                transactions documentation confirms a transaction may span warehouses in the same
--                workspace *including* reads from a lakehouse endpoint, so this is a supported
--                read and not a copy job in disguise.
--   Locally      `stg` is filled by the bridge in `src/lib/gold.py`, which reads the silver Delta
--                tables and writes them here.
--
-- So the procs are identical on both substrates. `docs/fabric-deployment.md` states that the bridge
-- is the only step that disappears on Fabric; this file is why that sentence is true.
--
-- ## Why stage at all, when Fabric could read silver directly
--
-- Three reasons, in descending order of how much they would cost to give up:
--
-- 1. **Snapshot stability.** A gold load reads `fact_transaction` four times (facts, the merchant
--    aggregate, and twice for reconciliation). Silver is being written by Spark on its own
--    schedule. Snapshot isolation inside the Warehouse protects reads *of Warehouse tables*; it
--    does not freeze a lakehouse table that Spark is appending to mid-transaction. Staging makes
--    the whole gold load read one consistent picture.
-- 2. **One seam, not seven.** The alternative — every proc referencing `lh_silver.dbo.x` on Fabric
--    and something else locally — is seven files of conditional SQL, which is how a portability
--    claim quietly becomes untrue.
-- 3. **Truncate-and-fill is a diagnosable state.** When gold is wrong, `stg` is the input that
--    produced it, still sitting there. Reading silver directly leaves nothing to inspect.
--
-- ## Shape
--
-- These tables mirror the silver Delta schemas **exactly**, including the `_`-prefixed control
-- columns, and deliberately do not rename anything. Renaming happens in the procs, where the
-- mapping is visible next to the target column — `dob` → `date_of_birth`, `status` →
-- `account_status`, `category` → `mcc_category`. A rename here would hide it.
--
-- Truncated at the start of every load. No constraints, no keys, no statistics: nothing reads these
-- tables except the proc that consumes them once, and a key on a table that lives for one
-- transaction is cost with no reader.
--
-- Types are the Fabric-supported equivalents of the silver Spark types: `string` → `varchar(n)`,
-- `long` → `bigint`, `timestamp` → `datetime2(6)`, `boolean` → `bit`. Widths match
-- `../ddl/02_dimensions.sql` and `../ddl/03_facts.sql` so that no proc performs a narrowing cast
-- it did not intend — a silent truncation at this boundary would be indistinguishable from a
-- source-data problem.
-- ============================================================================================

-- --------------------------------------------------------------------------------------------
-- SCD2 dimensions. Silver owns the history; these carry it across unchanged.
--
-- `valid_from` / `valid_to` / `is_current` / `_scd_hash` are *inputs* to gold, not something gold
-- recomputes. That division of labour is the most important thing about the dimension procs: silver
-- decided when a version began and ended, and gold's only job is to give each version a surrogate
-- key and keep the interval in sync. Recomputing SCD2 here would mean two implementations of the
-- hardest logic in the repo, differing in edge cases nobody would find until a fact joined to the
-- wrong version.
-- --------------------------------------------------------------------------------------------

CREATE TABLE stg.dim_account (
    account_id            varchar(20)   NULL,
    customer_id           varchar(20)   NULL,
    sort_code             varchar(6)    NULL,
    account_number_masked varchar(20)   NULL,
    account_type          varchar(20)   NULL,
    region                varchar(20)   NULL,
    opened_date           date          NULL,
    risk_band             varchar(1)    NULL,
    status                varchar(20)   NULL,
    credit_limit_minor    bigint        NULL,
    closed_date           date          NULL,
    _scd_hash             bigint        NULL,
    valid_from            datetime2(6)  NULL,
    valid_to              datetime2(6)  NULL,
    is_current            bit           NULL,
    _batch_id             varchar(100)  NULL,
    _updated_ts           datetime2(6)  NULL
);

CREATE TABLE stg.dim_customer (
    customer_id      varchar(20)   NULL,
    first_name       varchar(100)  NULL,
    last_name        varchar(100)  NULL,
    email            varchar(320)  NULL,
    dob              date          NULL,
    kyc_status       varchar(20)   NULL,
    country_code     varchar(2)    NULL,
    segment          varchar(20)   NULL,
    marketing_opt_in bit           NULL,
    _scd_hash        bigint        NULL,
    valid_from       datetime2(6)  NULL,
    valid_to         datetime2(6)  NULL,
    is_current       bit           NULL,
    _batch_id        varchar(100)  NULL,
    _updated_ts      datetime2(6)  NULL
);

CREATE TABLE stg.dim_merchant (
    merchant_id     varchar(20)   NULL,
    mcc             varchar(4)    NULL,
    merchant_name   varchar(100)  NULL,
    category        varchar(50)   NULL,
    country_code    varchar(2)    NULL,
    acquirer_id     varchar(20)   NULL,
    risk_score      int           NULL,
    status          varchar(20)   NULL,
    onboarded_date  date          NULL,
    _scd_hash       bigint        NULL,
    valid_from      datetime2(6)  NULL,
    valid_to        datetime2(6)  NULL,
    is_current      bit           NULL,
    _batch_id       varchar(100)  NULL,
    _updated_ts     datetime2(6)  NULL
);

-- --------------------------------------------------------------------------------------------
-- SCD1 and reference feeds. These arrive with bronze's audit columns rather than SCD2 columns,
-- because silver overwrites them rather than versioning them (`card_products` is static;
-- `fx_rates` is append-only reference data keyed on a date).
-- --------------------------------------------------------------------------------------------

CREATE TABLE stg.dim_card_product (
    card_product_code varchar(10)   NULL,
    product_name      varchar(50)   NULL,
    network           varchar(20)   NULL,
    tier              varchar(20)   NULL,
    annual_fee_minor  bigint        NULL,
    active_from       date          NULL,
    active_to         date          NULL,
    ingest_date       date          NULL,
    _source_file      varchar(1000) NULL,
    _batch_id         varchar(100)  NULL,
    _ingest_ts        datetime2(6)  NULL,
    _row_hash         bigint        NULL,
    _silver_batch_id  varchar(100)  NULL,
    _silver_ts        datetime2(6)  NULL
);

CREATE TABLE stg.dim_fx_rate (
    rate_date        date           NULL,
    from_currency    varchar(3)     NULL,
    to_currency      varchar(3)     NULL,
    rate             decimal(18,8)  NULL,
    source           varchar(30)    NULL,
    ingest_date      date           NULL,
    _source_file     varchar(1000)  NULL,
    _batch_id        varchar(100)   NULL,
    _ingest_ts       datetime2(6)   NULL,
    _row_hash        bigint         NULL,
    _silver_batch_id varchar(100)   NULL,
    _silver_ts       datetime2(6)   NULL
);

-- --------------------------------------------------------------------------------------------
-- Facts.
--
-- `fact_transaction` is the only table here whose staged volume is interesting: at `demo` scale it
-- is ~2M rows per full load. A daily incremental stages only the silver rows in the reprocessing
-- window, which is what `src/lib/gold.py` passes down as a date bound — the procs themselves never
-- decide *which* rows to load, only what to do with the ones they were given. That separation is
-- why the same proc serves a backfill and a nightly run.
-- --------------------------------------------------------------------------------------------

CREATE TABLE stg.fact_transaction (
    account_id          varchar(20)   NULL,
    amount_minor        bigint        NULL,
    auth_ts             datetime2(6)  NULL,
    capture_ts          datetime2(6)  NULL,
    card_id             varchar(36)   NULL,
    card_product_code   varchar(10)   NULL,
    channel             varchar(10)   NULL,
    country_code        varchar(2)    NULL,
    currency_code       varchar(3)    NULL,
    decline_reason_code varchar(30)   NULL,
    device_id           varchar(36)   NULL,
    is_3ds              bit           NULL,
    mcc                 varchar(4)    NULL,
    merchant_id         varchar(20)   NULL,
    settlement_date     date          NULL,
    status              varchar(20)   NULL,
    transaction_id      varchar(36)   NULL,
    wallet_type         varchar(20)   NULL,
    ingest_date         date          NULL,
    _source_file        varchar(1000) NULL,
    _batch_id           varchar(100)  NULL,
    _ingest_ts          datetime2(6)  NULL,
    _row_hash           bigint        NULL,
    _silver_batch_id    varchar(100)  NULL,
    _silver_ts          datetime2(6)  NULL
);

CREATE TABLE stg.fact_dispute (
    _event_ts             datetime2(6)  NULL,
    currency_code         varchar(3)    NULL,
    dispute_id            varchar(36)   NULL,
    disputed_amount_minor bigint        NULL,
    raised_date           date          NULL,
    reason_code           varchar(30)   NULL,
    resolved_date         date          NULL,
    status                varchar(20)   NULL,
    transaction_id        varchar(36)   NULL,
    ingest_date           date          NULL,
    _source_file          varchar(1000) NULL,
    _batch_id             varchar(100)  NULL,
    _ingest_ts            datetime2(6)  NULL,
    _row_hash             bigint        NULL,
    _silver_batch_id      varchar(100)  NULL,
    _silver_ts            datetime2(6)  NULL
);

-- --------------------------------------------------------------------------------------------
-- Load bookkeeping.
--
-- The Warehouse side of `meta_run_log`, not a replacement for it. The Spark control plane records
-- the notebook steps; this records the proc steps, because on Fabric the procs run as Stored
-- Procedure activities in the pipeline and a Spark table is the wrong place for a T-SQL step to
-- report — writing it would need a separate Spark session for the sole purpose of logging.
--
-- `load_batch_id` is carried into every gold row, which is what makes a gold load traceable back to
-- the silver batch that produced it. It is also the column a rollback would target, and the reason
-- every proc takes it as a parameter rather than deriving it from the clock: a retry of the same
-- logical load must carry the same id, for exactly the reason bronze's `_batch_id` must.
-- --------------------------------------------------------------------------------------------

CREATE TABLE stg.load_log (
    load_batch_id varchar(100)  NOT NULL,
    proc_name     varchar(128)  NOT NULL,
    started_ts    datetime2(6)  NOT NULL,
    finished_ts   datetime2(6)  NULL,
    rows_read     bigint        NULL,
    rows_inserted bigint        NULL,
    rows_updated  bigint        NULL,
    status        varchar(20)   NOT NULL,
    message       varchar(4000) NULL
);
