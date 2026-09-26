-- =====================================================================================
-- sp_load_dim_card_product — SCD1, overwrite in place
-- =====================================================================================
-- See 00_sp_seed_reference_data.sql for the proc contract this file follows.
--
-- docs/data-contracts.md types `card_products` as a static CSV loaded as a full snapshot and
-- treated as SCD1 in silver: no history, the newest value wins. This proc is therefore the
-- simplest of the loads, and it is second in the directory because it establishes the
-- surrogate-key pattern the three SCD2 dimensions reuse without the SCD2 machinery on top.
--
-- WHY THIS IS NOT A TRUNCATE-AND-REBUILD
--
-- A full-snapshot SCD1 dimension looks like the obvious candidate for TRUNCATE + INSERT, and
-- that would be a bug. The surrogate key here is positional — ROW_NUMBER over the business key
-- — so a rebuild reassigns every key the moment the set of products changes. Add a product
-- whose code sorts in the middle and every key after it shifts by one, silently repointing
-- historical fact rows at the wrong product. Nothing would catch it: the DDL's foreign keys are
-- NOT ENFORCED (they must be, on Fabric), every key still exists, and every join still
-- resolves. The numbers would simply be wrong.
--
-- So the load is update-in-place for existing codes plus an append for new ones, which is what
-- SCD1 actually means: overwrite the *attributes*, keep the identity. The only dimensions this
-- repo rebuilds wholesale are dim_date, whose key is derived from the date and so reproduces
-- exactly, and dim_fx_rate, which no fact references by key.
--
-- WHY ROW_NUMBER + MAX RATHER THAN IDENTITY
--
-- IDENTITY *is* supported on Warehouse in Microsoft Fabric — this repo's earlier notes had that
-- wrong and the linter does not ban it. The reason it is still not used is determinism, not
-- support. Fabric's IDENTITY allocation is gappy and does not guarantee the order in which
-- values are assigned across a distributed insert, so re-running a load would hand the same
-- business key a different surrogate. For a warehouse whose central claim is that a rerun
-- produces byte-identical output, a key generator that is allowed to disagree with itself is
-- the wrong tool. ROW_NUMBER over an explicit ORDER BY on the business key is reproducible.
--
-- What the pattern guarantees, precisely: keys are unique, positive, and stable once assigned.
-- Keys are *not* contiguous — a failed load that rolled back still consumed nothing, but a
-- product deleted from the source leaves its key behind forever — and the key a *new* row
-- receives depends on what else was staged in the same batch. Both are fine for a surrogate;
-- neither should be relied on by anything.
--
-- WHAT HAPPENS TO A PRODUCT THAT DISAPPEARS FROM THE SNAPSHOT
--
-- Nothing. It keeps its row and its key. Deleting it would orphan every historical transaction
-- that used it, and `active_to` already exists to express "no longer offered" — which is the
-- honest representation, because a withdrawn card product's past transactions did not stop
-- having happened. There is deliberately no WHEN NOT MATCHED BY SOURCE branch below.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_dim_card_product
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_dim_card_product';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_updated  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.dim_card_product;

        -- Counted before the MERGE, over the same predicate the MERGE uses. Under snapshot
        -- isolation this count and the MERGE see the same data, so the two agree.
        SELECT @rows_updated = COUNT_BIG(*)
          FROM dbo.dim_card_product AS tgt
         INNER JOIN stg.dim_card_product AS src
            ON tgt.card_product_code = src.card_product_code
         WHERE tgt.product_name <> src.product_name
            OR tgt.network <> src.network
            OR tgt.tier <> src.tier
            OR tgt.annual_fee_minor <> src.annual_fee_minor
            OR tgt.active_from <> src.active_from
            OR COALESCE(tgt.active_to, '9999-12-31') <> COALESCE(src.active_to, '9999-12-31');

        -- COALESCE on active_to rather than the three-way IS NULL comparison it replaces, and
        -- the choice is not just brevity: `tgt.active_to <> src.active_to` is UNKNOWN whenever
        -- either side is NULL, so a product being withdrawn — precisely the change this column
        -- exists to record — would not match the predicate and would never be updated. The
        -- sentinel makes the comparison total. It is safe because 9999-12-31 is outside the
        -- contracted domain of a real active_to.
        MERGE dbo.dim_card_product AS tgt
        USING stg.dim_card_product AS src
           ON tgt.card_product_code = src.card_product_code
        WHEN MATCHED AND (tgt.product_name <> src.product_name
                       OR tgt.network <> src.network
                       OR tgt.tier <> src.tier
                       OR tgt.annual_fee_minor <> src.annual_fee_minor
                       OR tgt.active_from <> src.active_from
                       OR COALESCE(tgt.active_to, '9999-12-31') <> COALESCE(src.active_to, '9999-12-31'))
        THEN UPDATE SET
            tgt.product_name     = src.product_name,
            tgt.network          = src.network,
            tgt.tier             = src.tier,
            tgt.annual_fee_minor = src.annual_fee_minor,
            tgt.active_from      = src.active_from,
            tgt.active_to        = src.active_to,
            tgt.load_batch_id    = @load_batch_id,
            tgt.loaded_ts        = @loaded_ts;

        -- The change predicate is repeated in the WHEN MATCHED clause rather than the MERGE
        -- updating unconditionally. An unconditional update would rewrite every row's
        -- load_batch_id on every run, which turns the provenance columns from "the load that
        -- last changed this row" into "the most recent load", and those answer different
        -- questions. It also makes the row count above meaningful.

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM stg.dim_card_product AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_card_product AS d
                            WHERE d.card_product_code = src.card_product_code);

        -- A separate INSERT rather than a WHEN NOT MATCHED BY TARGET branch on the MERGE above.
        -- The reason is the key: ROW_NUMBER is a window function over the source set, and a
        -- MERGE's insert branch has no such set to number — it sees one row at a time. Splitting
        -- the statement is what makes deterministic key assignment expressible at all.
        INSERT INTO dbo.dim_card_product
            (card_product_sk, card_product_code, product_name, network, tier,
             annual_fee_minor, active_from, active_to, load_batch_id, loaded_ts)
        SELECT ROW_NUMBER() OVER (ORDER BY src.card_product_code)
                 + (SELECT COALESCE(MAX(d.card_product_sk), 0)
                      FROM dbo.dim_card_product AS d
                     WHERE d.card_product_sk > 0),
               src.card_product_code,
               src.product_name,
               src.network,
               src.tier,
               src.annual_fee_minor,
               src.active_from,
               src.active_to,
               @load_batch_id,
               @loaded_ts
          FROM stg.dim_card_product AS src
         WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_card_product AS d
                            WHERE d.card_product_code = src.card_product_code);

        -- `WHERE d.card_product_sk > 0` excludes the -1 unknown member from the MAX. Without it
        -- the first load of an empty dimension would compute MAX = -1 and start numbering at 0,
        -- and 0 is a perfectly ordinary-looking key that no fact would ever be pointed at by a
        -- COALESCE(..., -1) lookup — a silent off-by-one that only shows up as one unjoinable
        -- product. The COALESCE(..., 0) handles the case where even the sentinel is absent.

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
