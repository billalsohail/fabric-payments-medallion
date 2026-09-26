-- =====================================================================================
-- sp_load_fact_dispute — the late-arriving fact, and why it borrows its keys
-- =====================================================================================
-- Contract: 00_sp_seed_reference_data.sql. One decision dominates this file.
--
-- THIS FACT INHERITS ITS DIMENSIONALITY FROM fact_transaction, NOT FROM THE DIMENSIONS
--
-- account_sk, merchant_sk, account_region and — most importantly — fx_rate are read from
-- dbo.fact_transaction by transaction_id, rather than resolved independently against the SCD2
-- dimensions the way 07 resolves them. That looks like a shortcut. It is the opposite: resolving
-- them here would be *wrong*, and wrong in a way no test on this table alone could detect.
--
-- A dispute is raised 0-90 days after its transaction (docs/data-contracts.md). If this proc
-- resolved account_sk against dim_account on raised_date, a dispute raised in June against a March
-- transaction would attribute itself to the account version current in June. The transaction sits on
-- the March version. Same account, two surrogate keys, and every measure that divides disputes by
-- transactions — chargeback rate in bps, the headline number on the merchant page — would be
-- computed across two different grains of the same dimension. The denominator and the numerator
-- would disagree about what an account is.
--
-- So the rule is: **a dispute is dimensioned as its transaction was.** The dispute's own dates are
-- still its own (raised_date_sk, resolved_date_sk); what it borrows is identity, not time.
--
-- The same argument, sharper, for FX. Reusing the transaction's fx_rate means
-- disputed_amount_gbp_minor and amount_gbp_minor are expressed in the same currency conversion, so
-- their ratio is a dispute rate. Converting the dispute at June's rate and the transaction at March's
-- would make that ratio a blend of a chargeback rate and three months of sterling movement, reported
-- as a chargeback rate. For a payments employer this is the single most consequential line in the
-- warehouse, and it is four characters of SQL: `t.fx_rate`.
--
-- The cost is a hard dependency: this proc must run after 07 in the same batch, because a dispute
-- whose transaction is not yet in the fact gets -1 keys and a NULL GBP amount. The orchestrator in
-- src/lib/gold.py enforces the order, and the reconciliation test asserts that the count of disputes
-- on account_sk = -1 equals the count whose transaction_id is genuinely absent from the fact — which
-- is not zero, because the generator injects dangling transaction_ids and the `referential` DQ rule
-- in silver records them rather than dropping them.
--
-- WHY resolved_date_sk IS -1 AND NOT NULL
--
-- An open dispute has no resolution date. ddl/03_facts.sql declares the column NOT NULL anyway and
-- points it at the unknown member, for the reason every key in this warehouse is NOT NULL: a NULL
-- foreign key in a star schema turns an inner join into a silent row filter, and the join that
-- disappears is the one a report author wrote without thinking about open disputes. -1 keeps the row
-- in the result set with "Unknown" against it, which is a question someone asks. A missing row is
-- not.
--
-- `is_open` is the column to filter on, and `resolution_days` is NULL for open disputes so that
-- AVG() over it means "average days to resolve, among resolved" without anyone writing a filter.
--
-- DELETE-THEN-INSERT, AND WHY THE WINDOW IS BIGGER THAN IT LOOKS
--
-- Silver reprocesses a rolling 90-day window for this feed, so staging carries every dispute raised
-- in the last 90 days, not just today's — including ones already in the fact and unchanged. Deleting
-- the staged dispute_ids and re-inserting them handles all three cases uniformly: genuinely new
-- disputes, disputes restated from OPEN to WON/LOST/WITHDRAWN, and unchanged rows rewritten
-- identically. The rewrite is wasted work on the unchanged majority and it is bought deliberately:
-- the alternative is a MERGE whose UPDATE branch lists every column and must keep agreeing with its
-- INSERT branch, to save writes on a table with orders of magnitude fewer rows than fact_transaction.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_fact_dispute
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_fact_dispute';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_deleted  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.fact_dispute;

        SELECT @rows_deleted = COUNT_BIG(*)
          FROM dbo.fact_dispute AS f
         WHERE f.dispute_id IN (SELECT s.dispute_id FROM stg.fact_dispute AS s);

        DELETE FROM dbo.fact_dispute
         WHERE dispute_id IN (SELECT s.dispute_id FROM stg.fact_dispute AS s);

        WITH staged AS (
            SELECT s.dispute_id,
                   s.transaction_id,
                   s.currency_code,
                   s.reason_code,
                   s.status,
                   s.raised_date,
                   s.resolved_date,
                   s.disputed_amount_minor,
                   (YEAR(s.raised_date) * 10000 + MONTH(s.raised_date) * 100
                        + DAY(s.raised_date)) AS raised_date_key,
                   -- NULL for an open dispute, and NULL propagates through the LEFT JOIN below to
                   -- COALESCE(..., -1) without a special case. The arithmetic does the work the
                   -- CASE would otherwise have to.
                   (YEAR(s.resolved_date) * 10000 + MONTH(s.resolved_date) * 100
                        + DAY(s.resolved_date)) AS resolved_date_key
              FROM stg.fact_dispute AS s
        ),
        keyed AS (
            SELECT staged.dispute_id,
                   staged.transaction_id,
                   staged.currency_code,
                   staged.reason_code,
                   staged.status,
                   staged.raised_date,
                   staged.resolved_date,
                   staged.disputed_amount_minor,
                   COALESCE(rd.date_sk, -1)            AS raised_date_sk,
                   COALESCE(sd.date_sk, -1)            AS resolved_date_sk,
                   -- Borrowed from the transaction. See the header: these are the transaction's
                   -- dimensional identity, not a fresh lookup on the dispute's own dates.
                   COALESCE(t.account_sk, -1)          AS account_sk,
                   COALESCE(t.merchant_sk, -1)         AS merchant_sk,
                   COALESCE(t.account_region, 'UNKNOWN') AS account_region,
                   t.fx_rate                           AS fx_rate,
                   COALESCE(cur.currency_sk, -1)       AS currency_sk,
                   cur.minor_unit_digits               AS minor_unit_digits
              FROM staged
              LEFT JOIN dbo.dim_date AS rd
                     ON rd.date_sk = staged.raised_date_key
              LEFT JOIN dbo.dim_date AS sd
                     ON sd.date_sk = staged.resolved_date_key
              LEFT JOIN dbo.fact_transaction AS t
                     ON t.transaction_id = staged.transaction_id
              -- The dispute's own currency, not the transaction's, because the amount being scaled
              -- is the disputed amount as the dispute feed stated it. The contract has them equal;
              -- taking it from the dispute means a future feed where they differ is converted
              -- correctly rather than plausibly.
              LEFT JOIN dbo.dim_currency AS cur
                     ON cur.currency_code = staged.currency_code
        ),
        converted AS (
            SELECT keyed.dispute_id,
                   keyed.transaction_id,
                   keyed.reason_code,
                   keyed.status,
                   keyed.raised_date,
                   keyed.resolved_date,
                   keyed.disputed_amount_minor,
                   keyed.raised_date_sk,
                   keyed.resolved_date_sk,
                   keyed.account_sk,
                   keyed.merchant_sk,
                   keyed.account_region,
                   keyed.currency_sk,
                   keyed.fx_rate,
                   CASE keyed.minor_unit_digits
                        WHEN 0 THEN CAST(100  AS decimal(12,4))
                        WHEN 2 THEN CAST(1    AS decimal(12,4))
                        WHEN 3 THEN CAST(0.1  AS decimal(12,4))
                        ELSE NULL END AS minor_unit_scale
              FROM keyed
        )
        INSERT INTO dbo.fact_dispute
            (dispute_id, transaction_id, raised_date_sk, resolved_date_sk,
             account_sk, merchant_sk, currency_sk, account_region,
             raised_date, resolved_date, resolution_days,
             disputed_amount_minor, disputed_amount_gbp_minor,
             is_open, is_won, is_lost, is_withdrawn,
             dispute_status, reason_code, load_batch_id, loaded_ts)
        SELECT dispute_id,
               transaction_id,
               raised_date_sk,
               resolved_date_sk,
               account_sk,
               merchant_sk,
               currency_sk,
               account_region,
               raised_date,
               resolved_date,
               -- NULL while open, which is what makes AVG(resolution_days) mean "among resolved"
               -- without a filter anyone could forget.
               DATEDIFF(day, raised_date, resolved_date),
               disputed_amount_minor,
               CAST(ROUND(disputed_amount_minor * fx_rate * minor_unit_scale, 0) AS bigint),
               CASE WHEN status = 'OPEN'      THEN 1 ELSE 0 END,
               CASE WHEN status = 'WON'       THEN 1 ELSE 0 END,
               CASE WHEN status = 'LOST'      THEN 1 ELSE 0 END,
               CASE WHEN status = 'WITHDRAWN' THEN 1 ELSE 0 END,
               status,
               reason_code,
               @load_batch_id,
               @loaded_ts
          FROM converted;

        -- The four flags are exhaustive and mutually exclusive here, unlike fact_transaction's,
        -- because dispute status *is* a state machine: a dispute is in exactly one of the four
        -- states. That means is_open + is_won + is_lost + is_withdrawn = 1 on every row, and the
        -- reconciliation test asserts it — a row summing to 0 is a status outside the contracted
        -- domain that silver let through.
        --
        -- "WON" is from the issuer's side: the cardholder's dispute succeeded and the merchant was
        -- debited. Worth stating because a merchant-facing report wants the same number called a
        -- loss, and the semantic model's measure names, not this column, are where that gets
        -- renamed for an audience.

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM dbo.fact_dispute AS f
         WHERE f.load_batch_id = @load_batch_id;

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
