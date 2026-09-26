-- =====================================================================================
-- sp_load_dim_merchant — SCD2, with a banded derived attribute
-- =====================================================================================
-- Contract: 00_sp_seed_reference_data.sql. SCD2 mechanics: 03_sp_load_dim_account.sql.
--
-- RENAMES AND DERIVATION
--
--   src.category → tgt.mcc_category     says what it is a category *of*
--   src.status   → tgt.merchant_status  disambiguated against three other status columns
--   risk_score   → tgt.risk_tier        a banding of the 0-100 score, derived here
--
-- WHY risk_tier IS BANDED IN GOLD AND WHAT THAT COSTS
--
-- The score is what the source provides and it is kept; the tier exists because almost nothing
-- downstream wants to group by 101 distinct values. Banding in the dimension rather than in a
-- measure means one definition of "high risk" for every report, which is the whole argument for
-- doing it at all.
--
-- It also has a consequence that is easy to miss, and it is the reason this comment is long.
-- `risk_tier` is computed at load time and stored per version, so **changing the band boundaries
-- does not restate history**. Worse, it does not restate it silently: the MERGE above re-syncs a
-- version only when `_scd_hash` moves, and the hash is computed in silver over *source* columns.
-- `risk_tier` is not a source column. So editing the CASE below changes the tier for versions
-- loaded afterwards and leaves every existing version on the old bands, with no row count, no log
-- line and no test failing.
--
-- Three options were considered. (a) Put the bands in a small lookup table and join at query time,
-- which makes a re-band instant and uniform, but moves the definition out of the model and into a
-- table nobody versions. (b) Recompute the tier for every row on every load, which makes the edit
-- take effect but rewrites the whole dimension nightly and destroys the meaning of
-- `load_batch_id`. (c) Band at load time and write down the limitation, which is what this does —
-- because band boundaries for a synthetic risk score change approximately never, and the honest
-- record of a known limitation is worth more than machinery for a change that will not happen.
--
-- If the bands ever do change, the fix is a one-off restatement: recompute `risk_tier` across the
-- dimension in a migration, not by editing this proc and waiting. That is a deliberate operational
-- cost, chosen with the eyes open.
--
-- The boundaries themselves: 0-24 LOW, 25-49 MEDIUM, 50-74 HIGH, 75-100 CRITICAL. Quarters of the
-- documented 0-100 domain, with no claim that they mean anything — the generator's score is
-- uniform, so a meaningful banding would be inventing risk semantics the data does not have.
-- `ELSE 'UNKNOWN'` catches a score outside the contracted domain rather than silently filing it as
-- CRITICAL, which is the sort of default that turns a data quality problem into a fraud alert.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_dim_merchant
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_dim_merchant';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_updated  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.dim_merchant;

        SELECT @rows_updated = COUNT_BIG(*)
          FROM dbo.dim_merchant AS tgt
         INNER JOIN stg.dim_merchant AS src
            ON tgt.merchant_id = src.merchant_id
           AND tgt.valid_from  = src.valid_from
         WHERE tgt.scd_hash   <> src._scd_hash
            OR tgt.valid_to   <> src.valid_to
            OR tgt.is_current <> src.is_current;

        MERGE dbo.dim_merchant AS tgt
        USING stg.dim_merchant AS src
           ON tgt.merchant_id = src.merchant_id
          AND tgt.valid_from  = src.valid_from
        WHEN MATCHED AND (tgt.scd_hash   <> src._scd_hash
                       OR tgt.valid_to   <> src.valid_to
                       OR tgt.is_current <> src.is_current)
        THEN UPDATE SET
            tgt.merchant_name   = src.merchant_name,
            tgt.mcc             = src.mcc,
            tgt.mcc_category    = src.category,
            tgt.country_code    = src.country_code,
            tgt.acquirer_id     = src.acquirer_id,
            tgt.risk_score      = src.risk_score,
            tgt.risk_tier       = CASE WHEN src.risk_score >= 0  AND src.risk_score < 25  THEN 'LOW'
                                       WHEN src.risk_score >= 25 AND src.risk_score < 50  THEN 'MEDIUM'
                                       WHEN src.risk_score >= 50 AND src.risk_score < 75  THEN 'HIGH'
                                       WHEN src.risk_score >= 75 AND src.risk_score <= 100 THEN 'CRITICAL'
                                       ELSE 'UNKNOWN' END,
            tgt.merchant_status = src.status,
            tgt.onboarded_date  = src.onboarded_date,
            tgt.valid_to        = src.valid_to,
            tgt.is_current      = src.is_current,
            tgt.scd_hash        = src._scd_hash,
            tgt.load_batch_id   = @load_batch_id,
            tgt.loaded_ts       = @loaded_ts;

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM stg.dim_merchant AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_merchant AS d
                            WHERE d.merchant_id = src.merchant_id
                              AND d.valid_from  = src.valid_from);

        INSERT INTO dbo.dim_merchant
            (merchant_sk, merchant_id, merchant_name, mcc, mcc_category, country_code,
             acquirer_id, risk_score, risk_tier, merchant_status, onboarded_date,
             valid_from, valid_to, is_current, scd_hash, load_batch_id, loaded_ts)
        SELECT ROW_NUMBER() OVER (ORDER BY src.merchant_id, src.valid_from)
                 + (SELECT COALESCE(MAX(d.merchant_sk), 0)
                      FROM dbo.dim_merchant AS d
                     WHERE d.merchant_sk > 0),
               src.merchant_id,
               src.merchant_name,
               src.mcc,
               src.category,
               src.country_code,
               src.acquirer_id,
               src.risk_score,
               CASE WHEN src.risk_score >= 0  AND src.risk_score < 25  THEN 'LOW'
                    WHEN src.risk_score >= 25 AND src.risk_score < 50  THEN 'MEDIUM'
                    WHEN src.risk_score >= 50 AND src.risk_score < 75  THEN 'HIGH'
                    WHEN src.risk_score >= 75 AND src.risk_score <= 100 THEN 'CRITICAL'
                    ELSE 'UNKNOWN' END,
               src.status,
               src.onboarded_date,
               src.valid_from,
               src.valid_to,
               src.is_current,
               src._scd_hash,
               @load_batch_id,
               @loaded_ts
          FROM stg.dim_merchant AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_merchant AS d
                            WHERE d.merchant_id = src.merchant_id
                              AND d.valid_from  = src.valid_from);

        -- The CASE is written twice, which is the one duplication in this proc worth defending.
        -- Fabric Warehouse has no scalar-UDF story worth relying on here (and a UDF could not use
        -- TRY...CATCH, so it would need its own error contract), and factoring it into a view over
        -- staging would put a gold business rule in a place the silver-owned schema does not
        -- describe. Two copies that must agree, in one file, twenty lines apart, with a comment
        -- above them saying so, is the least bad of the three.

        COMMIT TRAN;

        UPDATE stg.load_log
           SET finished_ts   = CAST(SYSDATETIME() AS datetime2(6)),
               rows_read     = @rows_read,
               rows_inserted = @rows_inserted,
               rows_updated  = @rows_updated,
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
