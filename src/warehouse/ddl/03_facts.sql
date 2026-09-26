-- =====================================================================================
-- wh_gold — 03: facts and aggregates
--
-- Same conventions as 02_dimensions.sql. Three tables, three grains, stated explicitly
-- because a fact table whose grain is not written down is a fact table that will be joined
-- wrongly:
--
--   dbo.fact_transaction    one authorisation attempt
--   dbo.fact_dispute        one dispute
--   dbo.agg_merchant_daily  one merchant per calendar day
--
-- **No fact table here has a surrogate primary key.** The grain is identified by its
-- degenerate business key (transaction_id, dispute_id), which is what the idempotent load
-- merges on. A meaningless bigint per fact row would cost 8 bytes per row across the widest
-- table in the warehouse and buy nothing: nothing joins to a fact, and the surrogate would
-- have to be assigned by a single-partition ROW_NUMBER, which is a full shuffle to one node
-- on an MPP engine. This is the one place the usual dimensional advice does not apply, and
-- it is worth saying so rather than letting it look like an omission.
--
-- **Nothing here is partitioned or distributed, and that is not an oversight.** Fabric
-- Warehouse does not expose DISTRIBUTION or PARTITION in CREATE TABLE at all — the engine
-- chooses physical layout itself, which is why tools/fabric_tsql_lint.py rejects the Synapse
-- syntax (FB101) that a reader coming from dedicated SQL pools would reasonably expect here.
-- At 100x this data the partition question would come back as a question about the *engine's*
-- choices, not the DDL's. See docs/design-decisions.md #10.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- fact_transaction — grain: one authorisation attempt
-- -------------------------------------------------------------------------------------
-- Not one payment. A decline followed by a successful retry is two rows, which is the whole
-- reason authorisation rate is measurable at all — collapse it to one row per payment and
-- the denominator disappears.
--
-- The dimension keys are resolved *as at the transaction's own auth_ts*, not as at today:
-- the SCD2 join is `f.auth_ts >= d.valid_from AND f.auth_ts < d.valid_to`. That is the
-- point of having built SCD2 dimensions, and getting it wrong here — joining on
-- is_current = 1 — would quietly restate history every time a risk band moved.
CREATE TABLE dbo.fact_transaction (
    -- Degenerate dimensions: business keys carried on the fact because they identify the
    -- grain and there is no dimension worth building for them.
    transaction_id          varchar(36)     NOT NULL,
    card_id                 varchar(36)     NOT NULL,

    -- Conformed dimension keys. All NOT NULL: an unresolved lookup lands on the -1 unknown
    -- member rather than becoming NULL, so that a fact row can never silently vanish from a
    -- sliced measure. decline_reason_sk resolves to -1 for every approved transaction, which
    -- is a legitimate 'not applicable' rather than a failed lookup — the two are
    -- indistinguishable in the key and distinguished by dbo.dim_decline_reason's own rows.
    date_sk                 int             NOT NULL,
    account_sk              bigint          NOT NULL,
    customer_sk             bigint          NOT NULL,
    merchant_sk             bigint          NOT NULL,
    card_product_sk         bigint          NOT NULL,
    -- Denormalised from dim_account purely so row-level security can be applied to this table
    -- directly. Without it the fact is protected only for queries that join the dimension, and
    -- `SELECT SUM(amount_minor) FROM fact_transaction` leaks the global total to every user —
    -- see the transitive-filtering note in 05_security.sql. Resolved from the *then-current*
    -- account version, so it carries the same point-in-time semantics as every other attribute
    -- here; an account that moves region does not retrospectively move its old transactions.
    account_region          varchar(20)     NOT NULL,
    currency_sk             bigint          NOT NULL,
    decline_reason_sk       bigint          NOT NULL,

    -- Event times. Three of them, and conflating any two is how late-arriving data gets
    -- reported in the wrong period: auth_ts is when the attempt happened, capture_ts when
    -- the merchant claimed the funds, settlement_date when money actually moved.
    auth_ts                 datetime2(6)    NOT NULL,
    capture_ts              datetime2(6)    NULL,
    settlement_date         date            NULL,
    settlement_lag_days     int             NULL,

    -- Measures. amount_minor is in the transaction's own currency; amount_gbp_minor is the
    -- reporting-currency conversion. Both are kept, because a sum of mixed-currency minor
    -- units is meaningless and a sum of converted amounts hides the rate that produced it.
    amount_minor            bigint          NOT NULL,
    amount_gbp_minor        bigint          NULL,
    fx_rate                 decimal(18,8)   NULL,
    -- 1 when the rate used was not published for this date but carried forward from an
    -- earlier one. docs/data-contracts.md requires the fill be flagged, not silent; this is
    -- that flag, and it is on the fact rather than the dimension because whether a rate was
    -- carried depends on the transaction's date, not on the rate.
    fx_rate_is_carried      bit             NULL,

    -- Additive flags, smallint rather than bit *because bit is not summable*. SUM(is_declined)
    -- is the decline count; SUM(CAST(bit AS int)) would be needed otherwise, and in Direct
    -- Lake a measure over a string status column scans that column's dictionary on every
    -- filter context. Storing the aggregation the report actually wants is the cheaper end of
    -- that trade, at 2 bytes a row.
    is_approved             smallint        NOT NULL,
    is_declined             smallint        NOT NULL,
    is_reversed             smallint        NOT NULL,
    is_settled              smallint        NOT NULL,

    -- Attributes kept on the fact: low-cardinality flags that no dimension would add
    -- anything to, plus status itself so the flags above can be audited against it.
    transaction_status      varchar(20)     NOT NULL,
    channel                 varchar(10)     NOT NULL,
    country_code            varchar(2)      NOT NULL,
    mcc                     varchar(4)      NOT NULL,
    is_3ds                  bit             NOT NULL,
    wallet_type             varchar(20)     NULL,
    device_id               varchar(36)     NULL,

    load_batch_id           varchar(100)    NOT NULL,
    loaded_ts               datetime2(6)    NOT NULL
);

-- wallet_type is nullable and that nullability is the schema-evolution story surfacing at
-- the serving layer: the column does not exist in the source files before month 10, so
-- every transaction older than that has no value for it. NULL here means 'the source did not
-- carry this field yet', which is a different fact from 'the customer used no wallet' — the
-- source spells the latter 'NONE'. Coalescing the two would erase the evidence that the
-- pipeline absorbed a mid-life schema change.

-- -------------------------------------------------------------------------------------
-- fact_dispute — grain: one dispute
-- -------------------------------------------------------------------------------------
-- A separate fact rather than columns on fact_transaction, because the grains differ: a
-- transaction has zero or one dispute, disputes arrive 0-90 days after the transaction, and
-- folding them in would mean rewriting settled fact rows for months afterwards. Keeping them
-- apart is what makes the transaction fact append-mostly.
--
-- transaction_id is carried so the two facts can be related; there is deliberately no
-- transaction surrogate key to join on, per the note at the top of this file.
CREATE TABLE dbo.fact_dispute (
    dispute_id                  varchar(36)     NOT NULL,
    transaction_id              varchar(36)     NOT NULL,

    raised_date_sk              int             NOT NULL,
    -- -1 (the unknown member) while the dispute is open and has no resolution date. This is
    -- the reason dim_date needs an unknown member at all, and the reason its sentinel row
    -- carries a real date rather than NULL: a NULL key would drop every open dispute from
    -- any measure sliced by resolution date, and open disputes are exactly the ones anybody
    -- cares about.
    resolved_date_sk            int             NOT NULL,
    account_sk                  bigint          NOT NULL,
    merchant_sk                 bigint          NOT NULL,
    currency_sk                 bigint          NOT NULL,
    -- Same reason as fact_transaction.account_region. Applied here too rather than only there,
    -- because a security control applied to one of two facts at the same sensitivity is not a
    -- control, it is an inconsistency someone will find.
    account_region              varchar(20)     NOT NULL,

    raised_date                 date            NOT NULL,
    resolved_date               date            NULL,
    resolution_days             int             NULL,

    disputed_amount_minor       bigint          NOT NULL,
    disputed_amount_gbp_minor   bigint          NULL,

    is_open                     smallint        NOT NULL,
    is_won                      smallint        NOT NULL,
    is_lost                     smallint        NOT NULL,
    is_withdrawn                smallint        NOT NULL,

    dispute_status              varchar(20)     NOT NULL,
    reason_code                 varchar(30)     NOT NULL,

    load_batch_id               varchar(100)    NOT NULL,
    loaded_ts                   datetime2(6)    NOT NULL
);

-- -------------------------------------------------------------------------------------
-- agg_merchant_daily — grain: one merchant per calendar day
-- -------------------------------------------------------------------------------------
-- A deliberate, and deliberately small, act of denormalisation. Direct Lake reads Delta
-- directly and is fast over the detail fact, so this table is not here to make the report
-- work — it is here because merchant-day is the grain the fraud and acquiring teams actually
-- monitor, and materialising it makes 'chargeback rate in bps by merchant-day' one scan
-- rather than a join between two facts at different grains.
--
-- The cost is real and worth naming: it is a second copy of the truth, and it goes stale the
-- moment a late dispute lands against an old transaction. sp_load_agg_merchant_daily
-- therefore rebuilds whole days rather than appending, and the reconciliation test asserts it
-- ties back to the detail. An aggregate nobody reconciles is a second set of numbers, not a
-- performance optimisation.
CREATE TABLE dbo.agg_merchant_daily (
    date_sk                     int             NOT NULL,
    merchant_sk                 bigint          NOT NULL,

    attempt_count               bigint          NOT NULL,
    approved_count              bigint          NOT NULL,
    declined_count              bigint          NOT NULL,
    distinct_account_count      bigint          NOT NULL,

    attempted_amount_gbp_minor  bigint          NOT NULL,
    approved_amount_gbp_minor   bigint          NOT NULL,

    dispute_count               bigint          NOT NULL,
    disputed_amount_gbp_minor   bigint          NOT NULL,

    load_batch_id               varchar(100)    NOT NULL,
    loaded_ts                   datetime2(6)    NOT NULL
);
