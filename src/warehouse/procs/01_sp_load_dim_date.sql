-- =====================================================================================
-- sp_load_dim_date — the generated calendar
-- =====================================================================================
-- The only dimension with no source feed at all: it is computed from two date parameters.
-- See 00_sp_seed_reference_data.sql for the proc contract this file follows.
--
-- WHY A TALLY CROSS JOIN RATHER THAN A RECURSIVE CTE
--
-- Because recursive queries are not supported on Warehouse in Microsoft Fabric. This is the
-- single most likely place for a developer to write T-SQL that runs perfectly on the local
-- SQL Server container and fails on deployment — `WITH d AS (SELECT @from UNION ALL SELECT
-- DATEADD(day,1,d) FROM d WHERE ...)` is *the* textbook date-dimension generator, and every
-- example on the internet uses it. FB015 in tools/fabric_tsql_lint.py exists to catch it, and
-- this file is the reason the rule was written before any proc was.
--
-- The replacement is four cross joins over a ten-row digit list, giving 10,000 consecutive day
-- offsets — about 27 years, which is a calendar, not a workaround. It is also strictly cheaper
-- than recursion: one pass, no iteration, and a distributed engine can parallelise it.
--
-- Only *sequential* CTEs are used. Nested CTEs (a WITH inside a WITH) are in preview on
-- Fabric, so FB017 warns on them; every derivation below is therefore a separate top-level CTE
-- reading the previous one.
--
-- WHY date_sk IS yyyymmdd AND EVERY OTHER DIMENSION'S KEY IS OPAQUE
--
-- A smart key is normally an anti-pattern — it invites code that computes business meaning from
-- a surrogate — but a date is the one dimension where the key is stable for all time, cannot
-- be restated, and has an unambiguous canonical encoding. The payoff is concrete: the fact
-- loads derive date_sk arithmetically from a timestamp instead of joining to this table, which
-- removes a join from the widest query in the warehouse. src/warehouse/ddl/02_dimensions.sql
-- states the same decision from the schema side.
--
-- TWO DELIBERATELY DATEFIRST-INDEPENDENT AND LANGUAGE-INDEPENDENT DERIVATIONS
--
-- day_of_week is computed as an offset from a known Monday rather than with
-- DATEPART(weekday, ...), because DATEPART(weekday) depends on the session's DATEFIRST setting
-- and SET DATEFIRST support on Fabric Warehouse is not documented. A dimension whose weekend
-- flag changes with a session option is a dimension that will be wrong for someone.
--
-- day_name and month_name are CASE expressions rather than DATENAME(...), because DATENAME is
-- language-dependent: the same load run under a different LANGUAGE setting writes 'Montag'.
-- These columns are report labels stored once and read by everybody, so they have to be
-- deterministic. English names are a choice; a session setting deciding them is not.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_dim_date
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6),
    @from_date      date,
    @to_date        date
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_dim_date';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_inserted bigint = 0;
    DECLARE @span_days int = DATEDIFF(day, @from_date, @to_date);
    DECLARE @msg varchar(2048);

    -- Checked before anything is logged, because this is a caller bug rather than a load
    -- failure: the tally below produces 10,000 offsets, so a wider span would silently
    -- truncate the calendar and the only symptom would be fact rows pointing at date keys that
    -- do not exist. A truncated calendar is far worse than a refused load — the DDL cannot
    -- enforce the foreign key, so nothing else would catch it.
    IF @span_days < 0 OR @span_days > 9999
    BEGIN
        SET @msg = CONCAT('sp_load_dim_date: @from_date..@to_date spans ', @span_days,
                          ' days; the tally supports 0..9999. Widen the tally or narrow the range.');
        THROW 50001, @msg, 1;
    END;

    -- The calendar is *not* derived from the data, and that is deliberate. A date dimension
    -- bounded by the facts breaks the moment a report asks for a future period or a
    -- late-arriving dispute lands a day beyond the last transaction. Its extent is a business
    -- decision, so the caller states it; orchestration/run.py pads it past the data on purpose.

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        -- A full rebuild, which is safe here for a reason that does not apply to the other
        -- dimensions: date_sk is computed from the date itself, so a rebuild reproduces every
        -- key exactly. No fact is ever orphaned by truncating this table. That property is what
        -- the smart key buys, and it is why this is the only dimension the procs may TRUNCATE
        -- without worrying about surrogate-key drift.
        TRUNCATE TABLE dbo.dim_date;

        -- The unknown member. Owned by this proc because this proc truncates the table — see
        -- the ownership rule in 00_sp_seed_reference_data.sql.
        --
        -- full_date carries a real date rather than NULL because the column is NOT NULL, and
        -- src/warehouse/ddl/03_facts.sql explains why the row has to exist at all:
        -- fact_dispute.resolved_date_sk is -1 for every open dispute, which is the majority of
        -- them at any moment. day_name and month_name say 'Unknown' so that a report slicing by
        -- name shows the fallback instead of quietly filing open disputes under January 1900.
        INSERT INTO dbo.dim_date
            (date_sk, full_date, day_of_month, day_of_week, day_name, day_of_year, iso_week,
             month_number, month_name, month_start_date, month_end_date, quarter_number,
             year_number, year_month, is_weekend, is_month_end)
        VALUES
            (-1, '1900-01-01', 1, 1, 'Unknown', 1, 1, 1, 'Unknown',
             '1900-01-01', '1900-01-31', 1, 1900, 190001, 0, 0);

        WITH digits AS (
            SELECT 0 AS n
            UNION ALL SELECT 1
            UNION ALL SELECT 2
            UNION ALL SELECT 3
            UNION ALL SELECT 4
            UNION ALL SELECT 5
            UNION ALL SELECT 6
            UNION ALL SELECT 7
            UNION ALL SELECT 8
            UNION ALL SELECT 9
        ),
        offsets AS (
            SELECT (d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000) AS day_offset
              FROM digits AS d1
             CROSS JOIN digits AS d2
             CROSS JOIN digits AS d3
             CROSS JOIN digits AS d4
        ),
        calendar AS (
            -- The bound is applied to the derived date rather than to day_offset so that the
            -- range is expressed once, in date terms, and cannot drift from @to_date.
            SELECT DATEADD(day, day_offset, @from_date) AS full_date
              FROM offsets
             WHERE DATEADD(day, day_offset, @from_date) <= @to_date
        ),
        parts AS (
            SELECT full_date,
                   YEAR(full_date)  AS year_number,
                   MONTH(full_date) AS month_number,
                   DAY(full_date)   AS day_of_month,
                   -- 1900-01-01 was a Monday, so this yields 1=Monday .. 7=Sunday for every
                   -- date at or after it. DATEDIFF is non-negative across that whole range,
                   -- which matters because T-SQL's % keeps the sign of its left operand.
                   ((DATEDIFF(day, CAST('1900-01-01' AS date), full_date) % 7) + 1) AS day_of_week,
                   DATEADD(day, 1 - DAY(full_date), full_date) AS month_start_date
              FROM calendar
        ),
        shaped AS (
            SELECT full_date, year_number, month_number, day_of_month, day_of_week,
                   month_start_date,
                   -- EOMONTH would be shorter; deriving the month end from the month start
                   -- keeps the two columns consistent by construction and needs no separate
                   -- support check.
                   DATEADD(day, -1, DATEADD(month, 1, month_start_date)) AS month_end_date
              FROM parts
        )
        INSERT INTO dbo.dim_date
            (date_sk, full_date, day_of_month, day_of_week, day_name, day_of_year, iso_week,
             month_number, month_name, month_start_date, month_end_date, quarter_number,
             year_number, year_month, is_weekend, is_month_end)
        SELECT CAST(year_number * 10000 + month_number * 100 + day_of_month AS int),
               full_date,
               CAST(day_of_month AS smallint),
               CAST(day_of_week AS smallint),
               CAST(CASE day_of_week
                        WHEN 1 THEN 'Monday'    WHEN 2 THEN 'Tuesday'  WHEN 3 THEN 'Wednesday'
                        WHEN 4 THEN 'Thursday'  WHEN 5 THEN 'Friday'   WHEN 6 THEN 'Saturday'
                        ELSE 'Sunday'
                    END AS varchar(10)),
               CAST(DATEPART(dayofyear, full_date) AS smallint),
               -- ISO week, not DATEPART(week). DATEPART(week) is also DATEFIRST-dependent and
               -- numbers a partial first week as week 1; iso_week is the standard every
               -- finance report actually means when it says "week 23".
               CAST(DATEPART(iso_week, full_date) AS smallint),
               CAST(month_number AS smallint),
               CAST(CASE month_number
                        WHEN  1 THEN 'January'   WHEN  2 THEN 'February' WHEN  3 THEN 'March'
                        WHEN  4 THEN 'April'     WHEN  5 THEN 'May'      WHEN  6 THEN 'June'
                        WHEN  7 THEN 'July'      WHEN  8 THEN 'August'   WHEN  9 THEN 'September'
                        WHEN 10 THEN 'October'   WHEN 11 THEN 'November' ELSE 'December'
                    END AS varchar(10)),
               month_start_date,
               month_end_date,
               CAST(DATEPART(quarter, full_date) AS smallint),
               CAST(year_number AS smallint),
               CAST(year_number * 100 + month_number AS int),
               CASE WHEN day_of_week >= 6 THEN 1 ELSE 0 END,
               CASE WHEN full_date = month_end_date THEN 1 ELSE 0 END
          FROM shaped;

        -- +1 for the unknown member, which is not in the tally.
        SELECT @rows_inserted = COUNT_BIG(*) FROM dbo.dim_date;

        COMMIT TRAN;

        UPDATE stg.load_log
           SET finished_ts   = CAST(SYSDATETIME() AS datetime2(6)),
               rows_read     = 0,
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
