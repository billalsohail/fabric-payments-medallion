-- =====================================================================================
-- sp_load_dim_customer — SCD2, with one derived attribute
-- =====================================================================================
-- Contract: 00_sp_seed_reference_data.sql. Key assignment, interval re-sync and the argument for
-- MERGE-plus-INSERT rather than `UPDATE ... FROM`: 03_sp_load_dim_account.sql. This file adds only
-- what is specific to customers.
--
-- RENAME AND DERIVATION
--
--   src.dob → tgt.date_of_birth   a source abbreviation becoming a model column name
--   YEAR(src.dob) → birth_year    a derived attribute that exists to be queryable when the column
--                                 it derives from is masked
--
-- `birth_year` is the interesting one. `date_of_birth` carries a `MASKED WITH` clause in
-- ddl/02_dimensions.sql, so an unprivileged principal reading it gets a default date — which makes
-- any age-based segmentation impossible for exactly the audience most likely to want it. A year of
-- birth is materially less identifying than a full date and is enough for cohort analysis, so it is
-- stored unmasked alongside the masked column.
--
-- That is a disclosure decision, not a modelling convenience, and it is worth stating plainly: this
-- deliberately makes one component of a PII column readable to everyone who can read the
-- dimension. It is defensible for a year of birth. It would not be defensible for a partial email
-- or the first half of a surname, and the reason to write the reasoning down is so that the next
-- person adding a "just the useful part" column has to make the same argument out loud.
--
-- Note also what masking is not: ddl/05_security.sql already says dynamic data masking is a
-- presentation feature, not an access control. The unmasked value is in the table and a principal
-- with UNMASK sees it. Nothing about birth_year changes the underlying protection, because there
-- was never any at this layer.
--
-- WHY YEAR() AND NOT DATEPART(year, ...)
--
-- They are the same function, but YEAR() cannot be handed a DATEFIRST- or language-dependent
-- datepart by a later edit. 01_sp_load_dim_date.sql explains at length why this repo avoids the
-- DATEPART family wherever a dedicated function exists; the habit is cheap and the failure it
-- prevents is silent.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_dim_customer
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_dim_customer';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_updated  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.dim_customer;

        SELECT @rows_updated = COUNT_BIG(*)
          FROM dbo.dim_customer AS tgt
         INNER JOIN stg.dim_customer AS src
            ON tgt.customer_id = src.customer_id
           AND tgt.valid_from  = src.valid_from
         WHERE tgt.scd_hash   <> src._scd_hash
            OR tgt.valid_to   <> src.valid_to
            OR tgt.is_current <> src.is_current;

        MERGE dbo.dim_customer AS tgt
        USING stg.dim_customer AS src
           ON tgt.customer_id = src.customer_id
          AND tgt.valid_from  = src.valid_from
        WHEN MATCHED AND (tgt.scd_hash   <> src._scd_hash
                       OR tgt.valid_to   <> src.valid_to
                       OR tgt.is_current <> src.is_current)
        THEN UPDATE SET
            tgt.first_name       = src.first_name,
            tgt.last_name        = src.last_name,
            tgt.email            = src.email,
            tgt.date_of_birth    = src.dob,
            tgt.birth_year       = YEAR(src.dob),
            tgt.kyc_status       = src.kyc_status,
            tgt.country_code     = src.country_code,
            tgt.segment          = src.segment,
            tgt.marketing_opt_in = src.marketing_opt_in,
            tgt.valid_to         = src.valid_to,
            tgt.is_current       = src.is_current,
            tgt.scd_hash         = src._scd_hash,
            tgt.load_batch_id    = @load_batch_id,
            tgt.loaded_ts        = @loaded_ts;

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM stg.dim_customer AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_customer AS d
                            WHERE d.customer_id = src.customer_id
                              AND d.valid_from  = src.valid_from);

        INSERT INTO dbo.dim_customer
            (customer_sk, customer_id, first_name, last_name, email, date_of_birth, birth_year,
             kyc_status, country_code, segment, marketing_opt_in,
             valid_from, valid_to, is_current, scd_hash, load_batch_id, loaded_ts)
        SELECT ROW_NUMBER() OVER (ORDER BY src.customer_id, src.valid_from)
                 + (SELECT COALESCE(MAX(d.customer_sk), 0)
                      FROM dbo.dim_customer AS d
                     WHERE d.customer_sk > 0),
               src.customer_id,
               src.first_name,
               src.last_name,
               src.email,
               src.dob,
               YEAR(src.dob),
               src.kyc_status,
               src.country_code,
               src.segment,
               src.marketing_opt_in,
               src.valid_from,
               src.valid_to,
               src.is_current,
               src._scd_hash,
               @load_batch_id,
               @loaded_ts
          FROM stg.dim_customer AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_customer AS d
                            WHERE d.customer_id = src.customer_id
                              AND d.valid_from  = src.valid_from);

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
