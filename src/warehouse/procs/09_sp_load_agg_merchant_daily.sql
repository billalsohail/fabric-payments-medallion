-- =====================================================================================
-- sp_load_agg_merchant_daily — the pre-aggregate, and the three ways it could lie
-- =====================================================================================
-- Contract: 00_sp_seed_reference_data.sql. This table exists because the merchant page of the report
-- asks for ten measures over a merchant-day grain, and computing them from 2M fact rows on every
-- visual interaction is what makes a Direct Lake model feel slow enough that someone switches it to
-- Import and reintroduces a refresh window. It is one table, ~8k merchants × 550 days at `demo`
-- scale, and it is the difference between a report that responds and a report people stop opening.
--
-- Pre-aggregates are also the easiest thing in a warehouse to get quietly wrong. Three specific ways,
-- and what this proc does about each.
--
-- 1. IT REBUILDS WHOLE DAYS, AND IT REBUILDS THEM FROM THE FACTS, NOT FROM STAGING
--
-- The obvious implementation aggregates `stg.fact_transaction` and adds the result to whatever is
-- already in the table. That is wrong for every batch after the first: transactions restate
-- (AUTHORISED → SETTLED, see 07), disputes arrive up to 90 days late, and either one means a day
-- already aggregated has changed. Adding a delta to a day whose underlying rows were *replaced*
-- double-counts them.
--
-- So: determine which days this batch touched, delete those days from the aggregate entirely, and
-- recompute them from `dbo.fact_transaction` and `dbo.fact_dispute` — which 07 and 08 have already
-- brought fully up to date inside this same orchestrator run. The aggregate is then a pure function
-- of the facts for every day it contains, which is the only property that makes it checkable: the
-- reconciliation test recomputes a sample of days from the facts and asserts equality, and that test
-- is meaningless unless the aggregate never holds a day that was assembled incrementally.
--
-- A dispute raised today against a March transaction therefore causes *March's* day to be rebuilt,
-- not today's. That is what `raised_date` driving the affected-day set is for, and it is the whole
-- reason the affected set is derived from the data rather than passed in as a date range.
--
-- 2. THE GRAIN IS THE UNION OF TWO KEY SETS, NOT THE TRANSACTION SIDE
--
-- A merchant-day with a dispute but no transactions is a real merchant-day: the dispute was raised
-- today against a transaction from March, so March has the transaction and today has the dispute.
-- Aggregating transactions and left-joining disputes onto them would drop it, and the row it drops is
-- a dispute — the rarest and most closely watched event in the warehouse. So the grain comes from a
-- UNION of both sides and both measures are left-joined onto it.
--
-- 3. EVERY COUNT AND SUM IS COALESCED TO ZERO, AND THE COLUMNS ARE NOT NULL
--
-- A merchant-day with no disputes has `dispute_count = 0`, never NULL. This is not tidiness: in DAX,
-- DIVIDE(SUM(dispute_count), SUM(attempt_count)) over a period containing NULLs gives a different
-- answer from the same expression over zeros in exactly the cases that matter, and a chargeback rate
-- that changes depending on whether a quiet merchant is included is not a rate. The DDL declares the
-- columns NOT NULL so that a missing COALESCE fails the insert instead of shipping.
--
-- The one measure that can still understate: `attempted_amount_gbp_minor` sums
-- `amount_gbp_minor`, which is NULL when FX did not resolve, and SUM silently skips NULLs. The
-- understatement is real but bounded and provable — `tests/test_gold_recon.py` asserts that the
-- count of facts with a NULL GBP amount is zero at `tiny` scale, so the skip is over an empty set.
-- If that assertion ever fails, it fails there, loudly, rather than showing up here as revenue that
-- went missing.
--
-- WHAT IS DELIBERATELY NOT HERE
--
-- No approval *rate*, no chargeback rate in bps, no averages. Only additive counts and sums. A rate
-- stored per merchant-day cannot be re-aggregated: averaging 550 daily approval rates is not the
-- approval rate for the period, and the error is largest for exactly the low-volume merchants a
-- fraud analyst is looking at. Rates belong in `semantic-model/measures.dax`, computed as
-- DIVIDE(SUM(numerator), SUM(denominator)) at whatever grain the user filtered to. Storing them here
-- would be faster and would produce wrong numbers, which is the trade every pre-aggregate is
-- tempted by.
--
-- `distinct_account_count` is the exception that proves the rule: it is *not* additive — the sum of
-- daily distinct accounts is not the monthly distinct accounts — and it is stored anyway, because
-- "distinct accounts on this day" is a question with an answer, and the semantic model marks the
-- measure as day-grain-only rather than pretending it rolls up. Naming the non-additive column is
-- better than omitting the measure or letting it be summed by accident.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_agg_merchant_daily
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_agg_merchant_daily';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_deleted  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        -- rows_read is the number of affected merchant-days about to be rebuilt, not a source row
        -- count: this proc has no staging table of its own, and "days touched" is the figure that
        -- explains the work. A batch of today's transactions plus one 90-day-late dispute reads as
        -- two days here, which is exactly the signal worth seeing in the log.
        SELECT @rows_read = COUNT_BIG(*)
          FROM (
            SELECT (YEAR(s.auth_ts) * 10000 + MONTH(s.auth_ts) * 100
                        + DAY(s.auth_ts)) AS date_sk
              FROM stg.fact_transaction AS s
            UNION
            SELECT (YEAR(d.raised_date) * 10000 + MONTH(d.raised_date) * 100
                        + DAY(d.raised_date)) AS date_sk
              FROM stg.fact_dispute AS d
          ) AS affected_days;

        SELECT @rows_deleted = COUNT_BIG(*)
          FROM dbo.agg_merchant_daily AS a
         WHERE a.date_sk IN (
            SELECT (YEAR(s.auth_ts) * 10000 + MONTH(s.auth_ts) * 100 + DAY(s.auth_ts))
              FROM stg.fact_transaction AS s
            UNION
            SELECT (YEAR(d.raised_date) * 10000 + MONTH(d.raised_date) * 100 + DAY(d.raised_date))
              FROM stg.fact_dispute AS d
         );

        -- The affected-day predicate is written out three times rather than materialised into a
        -- #temp table. Session-scoped temp tables are supported on Fabric Warehouse, so that option
        -- exists; it is declined because a temp table makes this proc's correctness depend on session
        -- state surviving between statements, and the orchestrator retries whole procs on write-write
        -- conflicts (24556/24706). Three copies of one predicate inside one transaction have no such
        -- dependency. They are identical by construction and the comment above each says so.
        DELETE FROM dbo.agg_merchant_daily
         WHERE date_sk IN (
            SELECT (YEAR(s.auth_ts) * 10000 + MONTH(s.auth_ts) * 100 + DAY(s.auth_ts))
              FROM stg.fact_transaction AS s
            UNION
            SELECT (YEAR(d.raised_date) * 10000 + MONTH(d.raised_date) * 100 + DAY(d.raised_date))
              FROM stg.fact_dispute AS d
         );

        WITH affected AS (
            SELECT (YEAR(s.auth_ts) * 10000 + MONTH(s.auth_ts) * 100
                        + DAY(s.auth_ts)) AS date_sk
              FROM stg.fact_transaction AS s
            UNION
            SELECT (YEAR(d.raised_date) * 10000 + MONTH(d.raised_date) * 100
                        + DAY(d.raised_date)) AS date_sk
              FROM stg.fact_dispute AS d
        ),
        tx AS (
            SELECT f.date_sk,
                   f.merchant_sk,
                   COUNT_BIG(*)                                  AS attempt_count,
                   CAST(SUM(f.is_approved) AS bigint)            AS approved_count,
                   CAST(SUM(f.is_declined) AS bigint)            AS declined_count,
                   COUNT_BIG(DISTINCT f.account_sk)              AS distinct_account_count,
                   SUM(f.amount_gbp_minor)                       AS attempted_amount_gbp_minor,
                   SUM(CASE WHEN f.is_approved = 1 THEN f.amount_gbp_minor ELSE 0 END)
                                                                 AS approved_amount_gbp_minor
              FROM dbo.fact_transaction AS f
             WHERE f.date_sk IN (SELECT a.date_sk FROM affected AS a)
             GROUP BY f.date_sk, f.merchant_sk
        ),
        dp AS (
            -- Grouped on raised_date_sk: a dispute belongs to the day it was raised, which is the
            -- day a fraud team acts on. resolved_date_sk drives a different measure (resolution
            -- time) and deliberately does not appear in this table at all, because a day's dispute
            -- count that changed retroactively when old disputes resolved would be unusable as a
            -- trend.
            SELECT f.raised_date_sk AS date_sk,
                   f.merchant_sk,
                   COUNT_BIG(*)                     AS dispute_count,
                   SUM(f.disputed_amount_gbp_minor) AS disputed_amount_gbp_minor
              FROM dbo.fact_dispute AS f
             WHERE f.raised_date_sk IN (SELECT a.date_sk FROM affected AS a)
             GROUP BY f.raised_date_sk, f.merchant_sk
        ),
        grains AS (
            SELECT tx.date_sk, tx.merchant_sk FROM tx
            UNION
            SELECT dp.date_sk, dp.merchant_sk FROM dp
        )
        INSERT INTO dbo.agg_merchant_daily
            (date_sk, merchant_sk, attempt_count, approved_count, declined_count,
             distinct_account_count, attempted_amount_gbp_minor, approved_amount_gbp_minor,
             dispute_count, disputed_amount_gbp_minor, load_batch_id, loaded_ts)
        SELECT g.date_sk,
               g.merchant_sk,
               COALESCE(tx.attempt_count, 0),
               COALESCE(tx.approved_count, 0),
               COALESCE(tx.declined_count, 0),
               COALESCE(tx.distinct_account_count, 0),
               COALESCE(tx.attempted_amount_gbp_minor, 0),
               COALESCE(tx.approved_amount_gbp_minor, 0),
               COALESCE(dp.dispute_count, 0),
               COALESCE(dp.disputed_amount_gbp_minor, 0),
               @load_batch_id,
               @loaded_ts
          FROM grains AS g
          LEFT JOIN tx ON tx.date_sk = g.date_sk AND tx.merchant_sk = g.merchant_sk
          LEFT JOIN dp ON dp.date_sk = g.date_sk AND dp.merchant_sk = g.merchant_sk;

        -- attempt_count = 0 with dispute_count > 0 is a legitimate row, not a defect: it is a day on
        -- which a merchant took no payments and a dispute landed against an older one. A NOT NULL
        -- constraint with a COALESCE is the whole mechanism that lets that row exist and read
        -- correctly.
        --
        -- merchant_sk = -1 aggregates ATM withdrawals and failed merchant lookups into one
        -- "Unknown" merchant-day, inheriting the ambiguity 07 accepted. That row is large and it is
        -- supposed to be — it is the volume that has no merchant, and hiding it by filtering
        -- merchant_sk > 0 here would make the aggregate disagree with the fact table on total
        -- volume, which is the first thing the reconciliation test checks.

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM dbo.agg_merchant_daily AS a
         WHERE a.load_batch_id = @load_batch_id;

        COMMIT TRAN;

        UPDATE stg.load_log
           SET finished_ts   = CAST(SYSDATETIME() AS datetime2(6)),
               rows_read     = @rows_read,
               rows_inserted = @rows_inserted,
               rows_updated  = @rows_deleted,
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
