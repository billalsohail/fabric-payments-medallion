-- =====================================================================================
-- sp_load_dim_account — SCD2, key assignment + interval re-sync
-- =====================================================================================
-- See 00_sp_seed_reference_data.sql for the proc contract this file follows, and
-- 02_sp_load_dim_card_product.sql for the surrogate-key pattern this one extends.
--
-- This is the file ddl/02_dimensions.sql forward-references. Three decisions are argued here
-- because all three SCD2 dimensions inherit them: what gold is actually responsible for, why the
-- keys are not IDENTITY, and why the load is a MERGE plus a separate INSERT rather than the
-- `UPDATE ... FROM` every SCD2 example on the internet uses.
--
-- 1. SILVER OWNS HISTORY. GOLD OWNS KEYS.
--
-- src/lib/scd2.py has already done the hard part. By the time rows reach stg.dim_account they are
-- versioned: `valid_from`, `valid_to`, `is_current` and `_scd_hash` are set, intervals are
-- half-open and contiguous except where a logical delete closed a chain, same-instant changes have
-- been collapsed, and out-of-order CDC has been refused rather than applied. tests/test_scd2.py
-- asserts all of that against the real generated feed.
--
-- So this proc does not detect changes, does not close out versions, and does not decide what a
-- version is. It does exactly two things: give each version a surrogate key, and keep gold's copy
-- of the intervals in step with silver's. Everything else would be a second SCD2 implementation,
-- and two implementations of SCD2 in one repo is two implementations to disagree.
--
-- The alternative — have gold derive history from a current-state feed — would duplicate logic
-- that is already tested against a feed containing reactivated keys, same-instant changes and
-- ~32% no-op updates. It is worth being explicit that this is a division of labour and not an
-- omission, because "the warehouse does the SCD2" is the shape most people expect.
--
-- 2. WHY THE KEYS ARE NOT IDENTITY
--
-- IDENTITY is supported on Warehouse in Microsoft Fabric. It is not used here, and the reason is
-- reproducibility rather than support: Fabric's allocation is gappy and the order in which values
-- are handed out across a distributed insert is not guaranteed, so the same version could receive
-- a different key on a re-run. Since the foreign keys in ddl/03_facts.sql are NOT ENFORCED — the
-- only form Fabric accepts — a shifted key does not fail anything. It silently repoints facts.
--
-- ROW_NUMBER() OVER (ORDER BY account_id, valid_from) + the current MAX is reproducible, and the
-- ORDER BY is over the business key and the interval start, which together are unique per version.
-- A tie in that ordering would make the assignment non-deterministic, so the pair matters:
-- (account_id, valid_from) is precisely the grain silver guarantees is unique.
--
-- What this pattern guarantees: keys are unique, positive, and stable once assigned. What it does
-- not guarantee: contiguity, and any relationship between key order and time beyond the order
-- within a single batch. A version loaded in a later batch gets a higher key than everything
-- before it, but two versions of the same account loaded in the same batch are numbered by
-- (account_id, valid_from) and not by arrival. Nothing should depend on either.
--
-- 3. WHY A MERGE PLUS A SEPARATE INSERT
--
-- The obvious SCD2 re-sync is `UPDATE dbo.dim_account SET valid_to = s.valid_to FROM stg.dim_account
-- AS s WHERE ...`. That statement does not run on Fabric Warehouse: the FROM clause is not
-- supported in an UPDATE, only single-table UPDATEs are. The linter enforces this as FB022, and
-- this is the proc that rule was written for — it would otherwise have been discovered on
-- deployment day, in five procs at once.
--
-- MERGE is GA on Warehouse and is the supported way to express "update this row from that row",
-- so the load is:
--
--   MERGE  — re-sync existing versions whose interval or attributes changed in silver
--   INSERT — add versions that gold has never seen, with keys
--
-- in that order. Re-syncing first means the INSERT's NOT EXISTS check runs against a dimension
-- that already agrees with silver about what exists, and freshly inserted rows are not immediately
-- re-examined by the MERGE. Reversing the two would still be correct but would do the work twice.
--
-- WHAT THE MERGE PREDICATE COMPARES, AND WHY IT INCLUDES THE HASH
--
-- The close-out case is the obvious one: a version that was current in the last batch is closed by
-- silver when a newer change arrives, so `valid_to` and `is_current` both move and gold must
-- follow. `scd_hash` is in the predicate for a different case — silver *restating* a version at the
-- same `valid_from`, which happens when an upstream correction changes an attribute without
-- changing when it took effect. Without the hash in the predicate gold would keep the old
-- attribute values forever, and nothing would report a difference, because the interval matched.
--
-- There is deliberately no WHEN NOT MATCHED BY SOURCE branch, and the reason is not the one you
-- would expect. stg.dim_account is staged in *full* (src/lib/gold.py declares the mode, because
-- SCD2 close-out does not restamp _batch_id, so there is no batch column that identifies a batch's
-- close-outs), so "absent from source" really does mean "absent from silver" and the branch would
-- not empty the dimension today.
--
-- It is omitted for two other reasons. First, the one row it would delete is the -1 unknown member:
-- no staged row carries account_id 'UNKNOWN', so it is absent from source by construction, and
-- every fact with an unresolved account points at it. Second, and more importantly, including the
-- branch would make this proc's correctness depend on the staging mode — a property declared in a
-- Python module, enforced by nothing in this file, and reasonable to change. A MERGE that is
-- correct only for full staging is a trap for whoever switches it. Gold accumulates; silver is the
-- source of what changed, not of what still exists.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_dim_account
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_dim_account';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_updated  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.dim_account;

        SELECT @rows_updated = COUNT_BIG(*)
          FROM dbo.dim_account AS tgt
         INNER JOIN stg.dim_account AS src
            ON tgt.account_id = src.account_id
           AND tgt.valid_from = src.valid_from
         WHERE tgt.scd_hash  <> src._scd_hash
            OR tgt.valid_to  <> src.valid_to
            OR tgt.is_current <> src.is_current;

        MERGE dbo.dim_account AS tgt
        USING stg.dim_account AS src
           ON tgt.account_id = src.account_id
          AND tgt.valid_from = src.valid_from
        WHEN MATCHED AND (tgt.scd_hash   <> src._scd_hash
                       OR tgt.valid_to   <> src.valid_to
                       OR tgt.is_current <> src.is_current)
        THEN UPDATE SET
            tgt.customer_id           = src.customer_id,
            tgt.sort_code             = src.sort_code,
            tgt.account_number_masked = src.account_number_masked,
            tgt.account_type          = src.account_type,
            tgt.account_status        = src.status,
            tgt.risk_band             = src.risk_band,
            tgt.credit_limit_minor    = src.credit_limit_minor,
            tgt.region                = src.region,
            tgt.opened_date           = src.opened_date,
            tgt.closed_date           = src.closed_date,
            tgt.valid_to              = src.valid_to,
            tgt.is_current            = src.is_current,
            tgt.scd_hash              = src._scd_hash,
            tgt.load_batch_id         = @load_batch_id,
            tgt.loaded_ts             = @loaded_ts;

        -- `src.status` → `tgt.account_status`. The rename happens here rather than in silver because
        -- silver's job is to stay recognisable against the source: docs/data-contracts.md types the
        -- accounts feed's column as `status`, and so does the dimension in silver. Gold is where
        -- three different `status` columns from three different feeds have to coexist in one model,
        -- and `account_status` / `merchant_status` / `transaction_status` is what makes a measure
        -- readable. Every rename in this layer is here for that reason and nowhere else.
        --
        -- `valid_from` is not in the SET list: it is half the join key, so it cannot differ.

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM stg.dim_account AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_account AS d
                            WHERE d.account_id = src.account_id
                              AND d.valid_from = src.valid_from);

        INSERT INTO dbo.dim_account
            (account_sk, account_id, customer_id, sort_code, account_number_masked,
             account_type, account_status, risk_band, credit_limit_minor, region,
             opened_date, closed_date, valid_from, valid_to, is_current, scd_hash,
             load_batch_id, loaded_ts)
        SELECT ROW_NUMBER() OVER (ORDER BY src.account_id, src.valid_from)
                 + (SELECT COALESCE(MAX(d.account_sk), 0)
                      FROM dbo.dim_account AS d
                     WHERE d.account_sk > 0),
               src.account_id,
               src.customer_id,
               src.sort_code,
               src.account_number_masked,
               src.account_type,
               src.status,
               src.risk_band,
               src.credit_limit_minor,
               src.region,
               src.opened_date,
               src.closed_date,
               src.valid_from,
               src.valid_to,
               src.is_current,
               src._scd_hash,
               @load_batch_id,
               @loaded_ts
          FROM stg.dim_account AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_account AS d
                            WHERE d.account_id = src.account_id
                              AND d.valid_from = src.valid_from);

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
