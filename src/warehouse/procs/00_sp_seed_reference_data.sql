-- =====================================================================================
-- sp_seed_reference_data — conformed lookups and the unknown members
-- =====================================================================================
-- This file is first in the directory because it also documents the contract every other
-- proc in src/warehouse/procs/ follows. Read it once and the other nine read quickly.
--
-- THE PROC CONTRACT
--
-- 1. Signature. Every proc takes (@load_batch_id varchar(100), @loaded_ts datetime2(6)) and
--    nothing else, except sp_load_dim_date which also takes the calendar bounds. The batch id
--    and timestamp are passed in rather than generated so that every table written by one
--    orchestrator run carries the same provenance — a proc calling SYSDATETIME() for its own
--    loaded_ts would stamp ten slightly different times on one logical load and make
--    "what did that run write?" unanswerable.
--
-- 2. Shape. DECLARE, BEGIN TRY, BEGIN TRAN, work, COMMIT TRAN, BEGIN CATCH, conditional
--    ROLLBACK, log, THROW. TRY...CATCH is supported on Warehouse in Microsoft Fabric, with
--    the constraint that a TRY block must be *immediately* followed by its CATCH and that the
--    pair cannot span an IF...ELSE — so the shape below is not stylistic, it is the only
--    arrangement the engine accepts. An IF *inside* the CATCH is fine and is the documented
--    pattern for the XACT_STATE() check.
--
-- 3. Transactions are anonymous. `BEGIN TRAN;` with no name, no save points, no marks: named,
--    save-point, marked and distributed transactions are all unsupported on Fabric Warehouse.
--    The consequence is worth stating rather than discovering: there is no partial recovery to
--    design around, so a proc either applies everything or nothing, and retry is the caller's
--    job. Fabric can raise write-write conflicts (errors 24556 / 24706) under snapshot
--    isolation even for append-only work, and the documented remedy is retry with exponential
--    backoff — which belongs in src/lib/gold.py, not in here, because a proc that retries
--    itself inside its own transaction cannot.
--
-- 4. Row counts use COUNT_BIG over the same predicate as the DML, computed inside the
--    transaction *before* the statement runs. Not @@ROWCOUNT: see FB023 in
--    tools/fabric_tsql_lint.py, but the short version is that BEGIN/COMMIT TRANSACTION reset
--    @@ROWCOUNT to 0, so a proc that interleaves transactions with DML — as all of these do —
--    would log 0 rows on a successful load. Counting the predicate also gives the number a
--    better meaning: "rows that qualified", not "rows the previous statement happened to
--    touch". Snapshot isolation guarantees the count and the DML see the same data.
--
-- 5. Every proc writes one row to stg.load_log, on success and on failure. The CATCH updates
--    the row it already inserted rather than inserting a second one, so a load has exactly one
--    log row per proc per batch whatever happened to it.
--
-- 6. No UPDATE ... FROM anywhere. Fabric Warehouse supports single-table UPDATE only; the
--    correlated updates the dimension procs need are expressed as MERGE. FB022 enforces it.
--
-- WHAT THIS PROC OWNS
--
-- Two kinds of row, and they are seeded together because they share one property: no upstream
-- feed produces them, so if this proc does not write them nothing will.
--
-- (a) The two warehouse-owned dimensions. dim_currency and dim_decline_reason have no landing
--     feed — docs/data-contracts.md says so explicitly for the latter. They are rebuilt whole
--     on every run, and their surrogate keys are **hard-coded literals** rather than
--     ROW_NUMBER. This is the one place the repo departs from its own key-assignment pattern
--     and the reason is the pattern's own failure mode: a rebuilt dimension whose keys are
--     positional gets different keys the moment a currency is added in the middle of the list,
--     silently repointing every historical fact at the wrong currency. Literal keys survive
--     editing this file.
--
-- (b) The unknown member (surrogate key -1) of every dimension no other proc truncates:
--     dim_account, dim_customer, dim_merchant and dim_card_product. Those four are loaded
--     incrementally and must keep their keys across runs, so their -1 row is inserted once,
--     guarded by WHERE NOT EXISTS, and never rewritten. dim_date owns its own -1 row because
--     sp_load_dim_date truncates; dim_fx_rate has none and the header of that proc says why.
--
-- The rule generalises: **a proc owns the unknown member of every table it truncates.** Split
-- differently, a TRUNCATE in one proc silently deletes a sentinel another proc believes it
-- placed, and the next fact load points thousands of rows at a key that no longer exists.
--
-- WHY AN UNKNOWN MEMBER AT ALL, RATHER THAN A NULL KEY
--
-- Because an inner join is the default and a NULL key drops the fact row. Unresolvable
-- dimension keys are a data-quality signal worth *reporting*, not one worth deleting revenue
-- over: a transaction whose merchant never arrived is still a real authorisation with a real
-- amount, and an analyst filtering to "Unknown merchant" should find it. Every dimension key
-- on fact_transaction and fact_dispute is therefore NOT NULL, and the lookup COALESCEs to -1.
--
-- Two consequences of that choice, both real and both accepted deliberately:
--
-- - "Not applicable" and "lookup failed" are indistinguishable in the key. An approved
--   transaction has no decline reason and an ATM withdrawal legitimately has no merchant;
--   both land on -1 alongside genuine failures. src/warehouse/ddl/03_facts.sql already made
--   this call for decline_reason_sk, and the distinction is recoverable from the fact's own
--   columns (transaction_status, channel) rather than from a second sentinel. Consistency with
--   that decision beats a -2 invented here.
-- - dim_account's unknown member carries region = 'UNKNOWN', and no principal is granted that
--   region in sec.user_region_access. Under the RLS predicate in 05_security.sql, facts that
--   fell back to the unknown account are therefore visible to *nobody* — which is safe but
--   means the reconciliation in tests/test_gold_recon.py has to run as a privileged principal
--   or it will miss exactly the rows it is looking for.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_seed_reference_data
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_seed_reference_data';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_inserted bigint = 0;

    -- Deleted before inserted so a replay of the same batch leaves one log row rather than a
    -- second one, per point 5 of the contract above. Outside the transaction deliberately: the
    -- log has to survive the ROLLBACK in the CATCH, which is the only circumstance in which
    -- anyone reads it.
    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        -- ---------------------------------------------------------------------------------
        -- dim_currency
        -- ---------------------------------------------------------------------------------
        -- minor_unit_digits is the column this dimension exists for. JPY has *zero* minor
        -- units: 1000 in a JPY amount_minor is 1000 yen, not 10.00. Every conversion to
        -- GBP minor units in sp_load_fact_transaction scales by 10^(2 - minor_unit_digits),
        -- and without this column that scaling would be a hard-coded 1 and every yen figure
        -- in the warehouse would be out by a factor of 100.
        --
        -- is_reporting_currency marks GBP as the currency the fact's *_gbp_minor columns are
        -- denominated in. It is a column rather than a literal 'GBP' scattered through the
        -- procs so that re-basing the warehouse to EUR is a data change.
        TRUNCATE TABLE dbo.dim_currency;
        INSERT INTO dbo.dim_currency
            (currency_sk, currency_code, currency_name, minor_unit_digits, is_reporting_currency)
        VALUES
            (-1, 'N/A', 'Unknown currency',   2, 0),
            ( 1, 'GBP', 'Pound sterling',     2, 1),
            ( 2, 'USD', 'US dollar',          2, 0),
            ( 3, 'EUR', 'Euro',               2, 0),
            ( 4, 'JPY', 'Japanese yen',       0, 0),
            ( 5, 'CHF', 'Swiss franc',        2, 0),
            ( 6, 'AUD', 'Australian dollar',  2, 0),
            ( 7, 'CAD', 'Canadian dollar',    2, 0),
            ( 8, 'SEK', 'Swedish krona',      2, 0),
            ( 9, 'NOK', 'Norwegian krone',    2, 0);

        -- The generator injects XYZ, ZZ1 and QQQ as invalid currency codes. Those rows are
        -- quarantined by the enum_domain DQ rule in silver and never reach gold; the -1 row
        -- above exists for the case where one somehow does, not as their expected home.

        -- ---------------------------------------------------------------------------------
        -- dim_decline_reason
        -- ---------------------------------------------------------------------------------
        -- is_retriable is the point of this table. Without it, "decline rate" is one number;
        -- with it, INSUFFICIENT_FUNDS (retry tomorrow, the customer's balance moves) is
        -- separable from SUSPECTED_FRAUD (retrying is the wrong action and may itself be a
        -- fraud signal). That split is the difference between a decline dashboard that
        -- describes a problem and one that suggests an action.
        --
        -- DO_NOT_HONOUR is marked NOT retriable, which is a judgement call worth stating: it
        -- is a generic issuer refusal with no stated cause, and in card operations a blind
        -- retry against it is the classic way to trip an issuer's velocity controls and make
        -- the decline permanent.
        TRUNCATE TABLE dbo.dim_decline_reason;
        INSERT INTO dbo.dim_decline_reason
            (decline_reason_sk, decline_reason_code, decline_reason_desc, decline_category, is_retriable)
        VALUES
            (-1, 'N/A',                'Not applicable or unknown',           'NONE',    0),
            ( 1, 'INSUFFICIENT_FUNDS', 'Insufficient funds',                  'FUNDING', 1),
            ( 2, 'LIMIT_EXCEEDED',     'Limit exceeded',                      'FUNDING', 1),
            ( 3, 'DO_NOT_HONOUR',      'Do not honour (generic issuer decline)', 'ISSUER',  0),
            ( 4, 'EXPIRED_CARD',       'Card expired',                        'CARD',    0),
            ( 5, 'INVALID_CVV',        'Invalid CVV',                         'CARD',    0),
            ( 6, 'SUSPECTED_FRAUD',    'Suspected fraud',                     'FRAUD',   0);

        -- ---------------------------------------------------------------------------------
        -- Unknown members for the four incrementally-loaded dimensions
        -- ---------------------------------------------------------------------------------
        -- WHERE NOT EXISTS rather than TRUNCATE-and-reinsert: these tables hold real versions
        -- by the time this proc runs a second time, and rebuilding them would reassign every
        -- surrogate key. The guard makes the proc idempotent without making it destructive.
        --
        -- valid_from = 1900-01-01 and valid_to = 9999-12-31 with is_current = 1, so the
        -- unknown member resolves under *any* point-in-time join. A sentinel that only
        -- resolves for part of history would fail the fact load for the rest of it.
        --
        -- The dates are real rather than NULL for the same reason dim_date's sentinel carries
        -- 1900-01-01: these columns are NOT NULL, and a sentinel that violates the schema it
        -- is meant to protect is not a sentinel.
        INSERT INTO dbo.dim_account
            (account_sk, account_id, customer_id, sort_code, account_number_masked,
             account_type, account_status, risk_band, credit_limit_minor, region,
             opened_date, closed_date, valid_from, valid_to, is_current, scd_hash,
             load_batch_id, loaded_ts)
        SELECT -1, 'UNKNOWN', 'UNKNOWN', '000000', '****0000',
               'UNKNOWN', 'UNKNOWN', 'U', NULL, 'UNKNOWN',
               '1900-01-01', NULL, '1900-01-01', '9999-12-31', 1, 0,
               @load_batch_id, @loaded_ts
        WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_account WHERE account_sk = -1);

        -- risk_band 'U' is outside the contracted A-E domain on purpose. A sentinel that
        -- borrowed a real band would make "how many accounts are risk band A" wrong, and the
        -- column is varchar(1) precisely so that a value outside the domain is still storable.

        INSERT INTO dbo.dim_customer
            (customer_sk, customer_id, first_name, last_name, email, date_of_birth, birth_year,
             kyc_status, country_code, segment, marketing_opt_in, valid_from, valid_to,
             is_current, scd_hash, load_batch_id, loaded_ts)
        SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown', 'unknown@example.invalid',
               '1900-01-01', 1900, 'UNKNOWN', 'ZZ', 'UNKNOWN', 0,
               '1900-01-01', '9999-12-31', 1, 0, @load_batch_id, @loaded_ts
        WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_customer WHERE customer_sk = -1);

        -- .invalid is reserved by RFC 2606 and can never be routable, so the sentinel email
        -- cannot accidentally receive mail if this dimension is ever fed to a campaign tool.
        -- country_code 'ZZ' is the ISO 3166-1 user-assigned code, which serves the same
        -- purpose: unmistakably not a real country.

        INSERT INTO dbo.dim_merchant
            (merchant_sk, merchant_id, merchant_name, mcc, mcc_category, country_code,
             acquirer_id, risk_score, risk_tier, merchant_status, onboarded_date,
             valid_from, valid_to, is_current, scd_hash, load_batch_id, loaded_ts)
        SELECT -1, 'UNKNOWN', 'Unknown merchant', '0000', 'Unknown', 'ZZ',
               'UNKNOWN', 0, 'UNKNOWN', 'UNKNOWN', '1900-01-01',
               '1900-01-01', '9999-12-31', 1, 0, @load_batch_id, @loaded_ts
        WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_merchant WHERE merchant_sk = -1);

        -- This is the sentinel that absorbs the ATM case: docs/data-contracts.md contracts
        -- merchant_id as nullable for ATM withdrawals, so ~0.5% of transactions resolve here
        -- legitimately. risk_score 0 rather than NULL keeps the column summable, and
        -- risk_tier 'UNKNOWN' keeps it out of the LOW/MEDIUM/HIGH bands so a tier breakdown
        -- shows the fallback rather than hiding it inside LOW.

        INSERT INTO dbo.dim_card_product
            (card_product_sk, card_product_code, product_name, network, tier,
             annual_fee_minor, active_from, active_to, load_batch_id, loaded_ts)
        SELECT -1, 'UNKNOWN', 'Unknown product', 'UNKNOWN', 'UNKNOWN',
               0, '1900-01-01', NULL, @load_batch_id, @loaded_ts
        WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_card_product WHERE card_product_sk = -1);

        -- Counted after the fact rather than accumulated per statement, because the two
        -- reference dimensions are rebuilt whole and the four sentinels are conditional: the
        -- honest number for this proc is "reference rows now present", not "rows this
        -- particular execution happened to write".
        SELECT @rows_inserted =
              (SELECT COUNT_BIG(*) FROM dbo.dim_currency)
            + (SELECT COUNT_BIG(*) FROM dbo.dim_decline_reason)
            + (SELECT COUNT_BIG(*) FROM dbo.dim_account       WHERE account_sk      = -1)
            + (SELECT COUNT_BIG(*) FROM dbo.dim_customer      WHERE customer_sk     = -1)
            + (SELECT COUNT_BIG(*) FROM dbo.dim_merchant      WHERE merchant_sk     = -1)
            + (SELECT COUNT_BIG(*) FROM dbo.dim_card_product  WHERE card_product_sk = -1);

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
        -- XACT_STATE() returns -1 for an uncommittable transaction and 1 for a committable
        -- one; either way there is something to roll back, and rolling back when there is
        -- nothing open is itself an error. Checking the state rather than @@TRANCOUNT is what
        -- makes this correct in the uncommittable case, where a transaction is open but only
        -- ROLLBACK is legal.
        IF XACT_STATE() <> 0 ROLLBACK TRAN;

        UPDATE stg.load_log
           SET finished_ts = CAST(SYSDATETIME() AS datetime2(6)),
               status      = 'FAILED',
               message     = CONCAT('error ', ERROR_NUMBER(), ' line ', ERROR_LINE(), ': ',
                                    ERROR_MESSAGE())
         WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;

        -- The log row is committed by the UPDATE above (we are outside the rolled-back
        -- transaction by this point), and only then does the error propagate. Re-raising is
        -- not optional: a proc that logs a failure and returns success lets the orchestrator
        -- carry on to the fact load with half a dimension.
        THROW;
    END CATCH
END;
