-- =====================================================================================
-- sp_load_dim_fx_rate — effective-dated rate intervals, rebuilt whole
-- =====================================================================================
-- Contract: 00_sp_seed_reference_data.sql.
--
-- This proc is the exception to three conventions the other nine follow, and each exception has a
-- reason that is specific to how rates are used rather than to how they arrive.
--
-- 1. IT READS THE WHOLE OF SILVER, NOT ONE BATCH
--
-- Every other proc treats stg.* as "the rows that changed in this batch". stg.dim_fx_rate is
-- loaded in full by the bridge in src/lib/gold.py, and that is a deliberate, documented difference
-- rather than an oversight — it is also the only staging table with that contract, so it is stated
-- here and asserted by tests/test_gold_recon.py.
--
-- The reason is the interval close. A rate's validity ends the day before the *next* rate for the
-- same currency pair, and the next rate arrives in a later batch than the row it closes. So an
-- incremental load would have to reach back and re-close the previous batch's final interval for
-- every pair it touched — which is `UPDATE ... FROM` shaped, and that statement is not supported on
-- Fabric Warehouse (FB022). Expressing it as a MERGE would mean staging a synthetic close-out row
-- per pair per batch purely to have something to match on. Reading the full history and computing
-- LEAD over it is both correct and shorter.
--
-- 2. IT TRUNCATES AND REBUILDS
--
-- Safe here, and nowhere else among the dimensions, because **no fact carries fx_rate_sk**. Check
-- ddl/03_facts.sql: fact_transaction stores the `fx_rate` it used and a `fx_rate_is_carried` flag,
-- not a key into this table. So reassigning every surrogate key on every load orphans nothing —
-- there is no join to break. The keys exist because a dimension in a star schema has one, and
-- because a semantic model needs something to relate on if a rate-explorer page is ever added.
--
-- That is also why this dimension has **no -1 unknown member**, which makes
-- ddl/02_dimensions.sql's blanket "every dimension carries an unknown member" one dimension too
-- broad. An unknown member exists so a failed lookup has somewhere to land; nothing looks this
-- table up by key, so there is no lookup that could fail.
--
-- 3. THE GAPS ARE THE POINT, AND THEY NEED NO SPECIAL CASE
--
-- docs/data-contracts.md injects five dates with no rates, and the `continuity` DQ rule catches
-- them at batch level because a missing row cannot be quarantined. The forward-fill is not code:
-- closing each interval at the day before the next rate means a gap simply produces an interval
-- several days wide, and a transaction inside it resolves to the last rate published before it.
-- That is what a rates desk actually does with a missing fixing.
--
-- `fact_transaction.fx_rate_is_carried` then falls out of comparing the transaction's date to the
-- interval's `rate_date` rather than being computed here — see 07_sp_load_fact_transaction.sql.
-- The flag belongs on the fact because carriedness is a property of the lookup, not of the rate:
-- the same interval is a same-day rate for one transaction and a three-day-old rate for another.
--
-- NO GBP → GBP ROW
--
-- The feed is `* → GBP` only and excludes GBP itself, and nothing synthesises the identity rate
-- here. A synthetic GBP row would need a rate_date per day to be joinable, so it would mean
-- generating a row per calendar day to express a constant. The fact load handles the reporting
-- currency with a CASE instead, which is one branch in one place.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_dim_fx_rate
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_dim_fx_rate';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.dim_fx_rate;

        TRUNCATE TABLE dbo.dim_fx_rate;

        WITH bounded AS (
            SELECT src.from_currency,
                   src.to_currency,
                   src.rate,
                   src.rate_date,
                   src.source,
                   LEAD(src.rate_date) OVER (PARTITION BY src.from_currency, src.to_currency
                                                 ORDER BY src.rate_date) AS next_rate_date
              FROM stg.dim_fx_rate AS src
        )
        INSERT INTO dbo.dim_fx_rate
            (fx_rate_sk, from_currency, to_currency, rate, rate_date,
             valid_from_date, valid_to_date, rate_source, load_batch_id, loaded_ts)
        SELECT ROW_NUMBER() OVER (ORDER BY from_currency, to_currency, rate_date),
               from_currency,
               to_currency,
               rate,
               rate_date,
               rate_date,
               COALESCE(DATEADD(day, -1, next_rate_date), '9999-12-31'),
               source,
               @load_batch_id,
               @loaded_ts
          FROM bounded;

        -- The intervals are **closed**, not half-open: valid_to_date is the last day the rate
        -- applies, so the fact join is `auth_date BETWEEN valid_from_date AND valid_to_date`. Every
        -- other effective-dated structure in this repo is half-open, and the inconsistency is
        -- deliberate — these bounds are dates, and a half-open date interval means writing the next
        -- interval's start as this one's end, which reads as though the rate changed a day early
        -- every time a human looks at the table. Half-open earns its keep for timestamps, where
        -- there is no "last instant" to name. For whole days there is, so it is named.
        --
        -- The final interval per pair ends at 9999-12-31 rather than being left open, because the
        -- BETWEEN join needs a bound and a sentinel the DDL declares NOT NULL cannot be NULL. A
        -- consequence worth stating: the newest rate is treated as valid forever, so a feed that
        -- stops silently converts stale rates into confident numbers. That is what the `freshness`
        -- DQ rule on this feed is for, and it is a different rule from `continuity` for exactly
        -- this reason — continuity catches interior gaps, freshness catches a feed that stopped.

        SELECT @rows_inserted = COUNT_BIG(*) FROM dbo.dim_fx_rate;

        COMMIT TRAN;

        UPDATE stg.load_log
           SET finished_ts   = CAST(SYSDATETIME() AS datetime2(6)),
               rows_read     = @rows_read,
               rows_inserted = @rows_inserted,
               rows_updated  = 0,
               status        = 'SUCCEEDED'
         WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;

        UPDATE stg.load_log
           SET finished_ts = CAST(SYSDATETIME() AS datetime2(6)),
               status      = 'FAILED',
               message     = CONCAT('error ', ERROR_NUMBER(), ' line ', ERROR_LINE(), ': ',
                                    ERROR_MESSAGE())
         WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;

        THROW;
    END CATCH
END;
