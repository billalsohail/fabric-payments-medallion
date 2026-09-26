-- =====================================================================================
-- sp_load_fact_transaction — key resolution, FX conversion, additive flags
-- =====================================================================================
-- Contract: 00_sp_seed_reference_data.sql. This is the proc the rest of the warehouse exists to
-- serve, and it is where five decisions that were only descriptions elsewhere become code.
--
-- 1. DELETE-THEN-INSERT, KEYED ON THE BUSINESS KEY
--
-- Not an append, and not a MERGE. A transaction is *restated*: the same transaction_id arrives
-- again days later as AUTHORISED → CAPTURED → SETTLED, each time with more of capture_ts,
-- settlement_date and status filled in. Appending would put three rows in the fact for one
-- authorisation attempt and double the volume measures. A MERGE would work, but its UPDATE branch
-- would have to list all thirty columns and keep agreeing with the INSERT branch forever — two
-- copies of the projection that must not drift. Deleting the staged ids and re-inserting them is
-- one projection, and it is the same delete-by-key idiom bronze uses for replay.
--
-- It is also what makes the load idempotent in the sense tests/test_idempotency.py asserts:
-- running the same batch twice deletes what the first run wrote and writes it again identically.
--
-- 2. EVERY DIMENSION LOOKUP IS A LEFT JOIN WITH COALESCE(..., -1)
--
-- Never an inner join. An inner join silently drops the fact row, which makes a broken lookup show
-- up as a volume shortfall discovered weeks later by someone reconciling against the source. The
-- -1 unknown member makes it show up as a row against "Unknown", which is visible on the first
-- report anyone opens. The DDL declares every key NOT NULL precisely so this choice cannot be
-- quietly abandoned — a missing COALESCE fails the insert rather than writing a NULL key.
--
-- One lookup is *expected* to miss: merchant_id is legitimately null for ATM withdrawals
-- (docs/data-contracts.md), so merchant_sk = -1 is correct data, not a defect. The same key
-- therefore means both "not applicable" and "lookup failed", which is the cost accepted in
-- ddl/03_facts.sql for decline_reason_sk as well. `channel = 'ATM'` distinguishes them, and a -2
-- "not applicable" member was rejected as inventing a distinction no report asks for.
--
-- 3. SCD2 LOOKUPS RESOLVE THE *THEN-CURRENT* VERSION
--
--   src.auth_ts >= dim.valid_from AND src.auth_ts < dim.valid_to
--
-- Half-open, on the authorisation instant, against three effective-dated dimensions. This is the
-- clause that makes SCD2 worth having: a transaction from March joins to the risk band the account
-- had in March, not to today's. Joining on is_current instead would make a fraud-rate-by-risk-band
-- trend an artefact of the most recent reband, which is the failure
-- tests/test_scd2.py::test_a_transaction_joins_to_the_then_current_version exists to prevent — and
-- this is the query that would have been wrong.
--
-- customer_sk is resolved in a second step because it chains: the transaction carries no
-- customer_id, only account_id, so the customer is whoever the *then-current account version* said
-- owned the account. Resolving it from a current-state account would attribute March's spend to
-- whoever holds the account now. A separate CTE rather than a nested join because sequential CTEs
-- are supported on Fabric Warehouse and nested ones are preview (FB017).
--
-- dim_card_product is SCD1, so it joins on code alone. Note what is deliberately *not* in that
-- join: `active_from`/`active_to`. A transaction on a product withdrawn last year still resolves to
-- that product, because the transaction happened. Filtering the join by the product's activity
-- window would send historical volume to the unknown member every time a product was retired.
--
-- 4. GBP CONVERSION SCALES BY MINOR UNITS
--
-- Money is in minor units everywhere, and minor units are not universal: JPY has none, so ¥1000 is
-- amount_minor = 1000, while $10.00 is amount_minor = 1000 as well. Multiplying either by a
-- GBP-per-unit rate and calling the result pence is wrong for one of them by a factor of 100. That
-- is why dim_currency carries minor_unit_digits, and the scale is 10^(2 - minor_unit_digits).
--
-- Written as a CASE over the three digit counts that exist rather than POWER(10, 2 - digits),
-- because POWER returns float and float has no business in the arithmetic that produces a money
-- column. `ELSE NULL` on an unexpected digit count yields a NULL GBP amount — a visible hole that
-- the reconciliation test catches — rather than a plausible wrong number.
--
-- GBP transactions take rate 1 by CASE rather than by a synthetic dimension row; see
-- 06_sp_load_dim_fx_rate.sql for why that row does not exist.
--
-- 5. FLAGS ARE smallint AND THEY ARE NOT MUTUALLY EXCLUSIVE IN THE OBVIOUS WAY
--
-- is_approved covers AUTHORISED, CAPTURED and SETTLED, because all three are attempts the issuer
-- said yes to — authorisation rate is approvals over attempts, and a transaction that settled was
-- approved. is_settled is a strict subset of is_approved, deliberately: the two answer different
-- questions and a report summing both is asking for both answers, not double-counting one.
-- REVERSED is neither approved nor declined here; it was authorised and then undone, and folding it
-- into approvals would overstate the rate while folding it into declines would understate what the
-- issuer actually decided. It gets its own flag and no opinion.
-- =====================================================================================

CREATE PROCEDURE dbo.sp_load_fact_transaction
    @load_batch_id  varchar(100),
    @loaded_ts      datetime2(6)
AS
BEGIN
    DECLARE @proc_name  varchar(128) = 'sp_load_fact_transaction';
    DECLARE @started_ts datetime2(6) = CAST(SYSDATETIME() AS datetime2(6));
    DECLARE @rows_read     bigint = 0;
    DECLARE @rows_inserted bigint = 0;
    DECLARE @rows_deleted  bigint = 0;

    DELETE FROM stg.load_log WHERE load_batch_id = @load_batch_id AND proc_name = @proc_name;
    INSERT INTO stg.load_log (load_batch_id, proc_name, started_ts, status)
    VALUES (@load_batch_id, @proc_name, @started_ts, 'RUNNING');

    BEGIN TRY
        BEGIN TRAN;

        SELECT @rows_read = COUNT_BIG(*) FROM stg.fact_transaction;

        -- Counted before the delete, and reported as rows_updated: from the warehouse's point of
        -- view a restated transaction is an update, and the number of staged rows that already
        -- existed is the one figure that says whether this batch was new volume or a correction.
        SELECT @rows_deleted = COUNT_BIG(*)
          FROM dbo.fact_transaction AS f
         WHERE f.transaction_id IN (SELECT s.transaction_id FROM stg.fact_transaction AS s);

        DELETE FROM dbo.fact_transaction
         WHERE transaction_id IN (SELECT s.transaction_id FROM stg.fact_transaction AS s);

        WITH staged AS (
            SELECT s.transaction_id,
                   s.card_id,
                   s.account_id,
                   s.merchant_id,
                   s.card_product_code,
                   s.currency_code,
                   s.decline_reason_code,
                   s.auth_ts,
                   s.capture_ts,
                   s.settlement_date,
                   s.amount_minor,
                   s.status,
                   s.channel,
                   s.country_code,
                   s.mcc,
                   s.is_3ds,
                   s.wallet_type,
                   s.device_id,
                   CAST(s.auth_ts AS date) AS auth_date,
                   -- The smart key is computed rather than joined for, which is the one thing
                   -- dim_date's key shape buys: no lookup on the highest-cardinality join in the
                   -- warehouse. It is still passed through a LEFT JOIN below so that a date outside
                   -- the generated calendar lands on -1 instead of pointing at a key that does not
                   -- exist. Arithmetic gives the value; the join proves it is real.
                   (YEAR(s.auth_ts) * 10000 + MONTH(s.auth_ts) * 100 + DAY(s.auth_ts)) AS date_key
              FROM stg.fact_transaction AS s
        ),
        keyed AS (
            SELECT staged.transaction_id,
                   staged.card_id,
                   staged.currency_code,
                   staged.auth_ts,
                   staged.auth_date,
                   staged.capture_ts,
                   staged.settlement_date,
                   staged.amount_minor,
                   staged.status,
                   staged.channel,
                   staged.country_code,
                   staged.mcc,
                   staged.is_3ds,
                   staged.wallet_type,
                   staged.device_id,
                   COALESCE(dd.date_sk, -1)             AS date_sk,
                   COALESCE(a.account_sk, -1)           AS account_sk,
                   COALESCE(a.region, 'UNKNOWN')        AS account_region,
                   a.customer_id                        AS owning_customer_id,
                   COALESCE(m.merchant_sk, -1)          AS merchant_sk,
                   COALESCE(cp.card_product_sk, -1)     AS card_product_sk,
                   COALESCE(cur.currency_sk, -1)        AS currency_sk,
                   cur.minor_unit_digits                AS minor_unit_digits,
                   COALESCE(dr.decline_reason_sk, -1)   AS decline_reason_sk,
                   fx.rate                              AS fx_rate_published,
                   fx.rate_date                         AS fx_rate_date
              FROM staged
              LEFT JOIN dbo.dim_date AS dd
                     ON dd.date_sk = staged.date_key
              LEFT JOIN dbo.dim_account AS a
                     ON a.account_id = staged.account_id
                    AND staged.auth_ts >= a.valid_from
                    AND staged.auth_ts <  a.valid_to
              LEFT JOIN dbo.dim_merchant AS m
                     ON m.merchant_id = staged.merchant_id
                    AND staged.auth_ts >= m.valid_from
                    AND staged.auth_ts <  m.valid_to
              LEFT JOIN dbo.dim_card_product AS cp
                     ON cp.card_product_code = staged.card_product_code
              LEFT JOIN dbo.dim_currency AS cur
                     ON cur.currency_code = staged.currency_code
              LEFT JOIN dbo.dim_decline_reason AS dr
                     ON dr.decline_reason_code = staged.decline_reason_code
              LEFT JOIN dbo.dim_fx_rate AS fx
                     ON fx.from_currency = staged.currency_code
                    AND fx.to_currency   = 'GBP'
                    AND staged.auth_date BETWEEN fx.valid_from_date AND fx.valid_to_date
        ),
        chained AS (
            SELECT keyed.transaction_id,
                   keyed.card_id,
                   keyed.currency_code,
                   keyed.auth_ts,
                   keyed.auth_date,
                   keyed.capture_ts,
                   keyed.settlement_date,
                   keyed.amount_minor,
                   keyed.status,
                   keyed.channel,
                   keyed.country_code,
                   keyed.mcc,
                   keyed.is_3ds,
                   keyed.wallet_type,
                   keyed.device_id,
                   keyed.date_sk,
                   keyed.account_sk,
                   keyed.account_region,
                   keyed.merchant_sk,
                   keyed.card_product_sk,
                   keyed.currency_sk,
                   keyed.minor_unit_digits,
                   keyed.decline_reason_sk,
                   keyed.fx_rate_published,
                   keyed.fx_rate_date,
                   COALESCE(c.customer_sk, -1) AS customer_sk
              FROM keyed
              LEFT JOIN dbo.dim_customer AS c
                     ON c.customer_id = keyed.owning_customer_id
                    AND keyed.auth_ts >= c.valid_from
                    AND keyed.auth_ts <  c.valid_to
        ),
        converted AS (
            SELECT chained.transaction_id,
                   chained.card_id,
                   chained.auth_ts,
                   chained.auth_date,
                   chained.capture_ts,
                   chained.settlement_date,
                   chained.amount_minor,
                   chained.status,
                   chained.channel,
                   chained.country_code,
                   chained.mcc,
                   chained.is_3ds,
                   chained.wallet_type,
                   chained.device_id,
                   chained.date_sk,
                   chained.account_sk,
                   chained.customer_sk,
                   chained.account_region,
                   chained.merchant_sk,
                   chained.card_product_sk,
                   chained.currency_sk,
                   chained.decline_reason_sk,
                   chained.fx_rate_date,
                   CASE WHEN chained.currency_code = 'GBP' THEN CAST(1 AS decimal(18,8))
                        ELSE chained.fx_rate_published END AS fx_rate,
                   CASE chained.minor_unit_digits
                        WHEN 0 THEN CAST(100  AS decimal(12,4))
                        WHEN 2 THEN CAST(1    AS decimal(12,4))
                        WHEN 3 THEN CAST(0.1  AS decimal(12,4))
                        ELSE NULL END AS minor_unit_scale
              FROM chained
        )
        INSERT INTO dbo.fact_transaction
            (transaction_id, card_id, date_sk, account_sk, customer_sk, merchant_sk,
             card_product_sk, account_region, currency_sk, decline_reason_sk,
             auth_ts, capture_ts, settlement_date, settlement_lag_days,
             amount_minor, amount_gbp_minor, fx_rate, fx_rate_is_carried,
             is_approved, is_declined, is_reversed, is_settled,
             transaction_status, channel, country_code, mcc, is_3ds, wallet_type, device_id,
             load_batch_id, loaded_ts)
        SELECT transaction_id,
               card_id,
               date_sk,
               account_sk,
               customer_sk,
               merchant_sk,
               card_product_sk,
               account_region,
               currency_sk,
               decline_reason_sk,
               auth_ts,
               capture_ts,
               settlement_date,
               -- capture → settlement, not auth → settlement. The measure is how long the money
               -- took to move after the merchant claimed it, and an authorisation that is never
               -- captured never settles. NULL until both ends exist, which DATEDIFF gives for free.
               DATEDIFF(day, CAST(capture_ts AS date), settlement_date),
               amount_minor,
               CAST(ROUND(amount_minor * fx_rate * minor_unit_scale, 0) AS bigint),
               fx_rate,
               -- Carriedness is a property of this lookup, not of the rate: the same interval is a
               -- same-day rate for one transaction and a four-day-old rate for another. NULL when no
               -- rate resolved at all, which is a third state and not the same as "not carried".
               CASE WHEN fx_rate_date IS NULL THEN NULL
                    WHEN fx_rate_date < auth_date THEN 1
                    ELSE 0 END,
               CASE WHEN status IN ('AUTHORISED', 'CAPTURED', 'SETTLED') THEN 1 ELSE 0 END,
               CASE WHEN status = 'DECLINED' THEN 1 ELSE 0 END,
               CASE WHEN status = 'REVERSED' THEN 1 ELSE 0 END,
               CASE WHEN status = 'SETTLED'  THEN 1 ELSE 0 END,
               status,
               channel,
               country_code,
               mcc,
               is_3ds,
               wallet_type,
               device_id,
               @load_batch_id,
               @loaded_ts
          FROM converted;

        -- GBP rows get minor_unit_scale = 1 from dim_currency's own minor_unit_digits = 2, and
        -- fx_rate = 1 from the CASE, so amount_gbp_minor = amount_minor exactly. No rounding, no
        -- drift on the reporting currency — which the reconciliation test relies on to tie
        -- GBP-normalised volume back to the raw amounts for the GBP subset.

        SELECT @rows_inserted = COUNT_BIG(*)
          FROM dbo.fact_transaction AS f
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
