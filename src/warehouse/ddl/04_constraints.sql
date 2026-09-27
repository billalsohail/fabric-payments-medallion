-- =====================================================================================
-- wh_gold — 04: constraints
--
-- **Every constraint in this file is `NOT ENFORCED`, and that is not a workaround — it is the
-- only form Fabric Data Warehouse accepts.** `PRIMARY KEY` and `UNIQUE` must be declared
-- `NONCLUSTERED NOT ENFORCED`; `FOREIGN KEY` must be declared `NOT ENFORCED`; `CHECK`,
-- `DEFAULT` and computed columns do not exist at all. tools/fabric_tsql_lint.py enforces this
-- (FB301, FB302 and FB303 for the constraints; FB104 and FB103 for DEFAULT and computed
-- columns), which is why the line holds across the whole repo rather than only where someone
-- remembered.
--
-- So what are these declarations *for*, if the engine will not police them?
--
--   1. **The optimiser reads them.** A trusted-by-declaration key lets the engine eliminate
--      joins and pick better cardinality estimates. Declaring them is a performance decision.
--   2. **Power BI reads them.** Relationship detection over the SQL analytics endpoint uses
--      declared FKs. Omitting them means hand-drawing every relationship in the model and
--      getting one wrong.
--   3. **A reader reads them.** They are the executable statement of the star's shape.
--
-- And what actually guarantees the integrity they describe?
--
-- **The DQ layer does.** That sentence is the honest answer and it is worth saying in exactly
-- those words, because the alternative — declaring keys and assuming something checks them —
-- is how a warehouse ends up with duplicate dimension rows and a fact table that fans out.
-- Concretely: `src/lib/dq.py`'s `unique` rule is what makes the PKs true, its `referential`
-- rule is what makes the FKs true, its `not_null` rule is what makes the NOT NULLs true, and
-- `tests/test_dq_gate.py` proves a violation fails the run rather than being logged and
-- forgotten. The integrity guarantee lives one layer upstream of where it is declared, and
-- `tests/test_gold_recon.py` re-checks it *here* after the load — belt and braces, because a
-- silver-side guarantee says nothing about a bug in the gold load itself.
--
-- The one thing `NOT ENFORCED` genuinely costs: a declared-but-violated key makes the
-- optimiser's join elimination *wrong*, not merely unhelpful, and the symptom is missing rows
-- rather than an error. That is the real argument for the post-load re-check.
--
-- Deployment order: after 02 and 03, before 05. ALTER TABLE ADD CONSTRAINT on a non-existent
-- table fails the batch.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Primary keys — dimensions
-- -------------------------------------------------------------------------------------
-- On the *surrogate* key, never the business key, and for the SCD2 dimensions that is the
-- whole point: `dim_account` holds several rows per account_id by design, so account_id is not
-- unique and cannot be the key. The uniqueness that does hold for those tables is
-- (business key, valid_from), declared as a UNIQUE constraint further down.

ALTER TABLE dbo.dim_date
    ADD CONSTRAINT pk_dim_date PRIMARY KEY NONCLUSTERED (date_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_currency
    ADD CONSTRAINT pk_dim_currency PRIMARY KEY NONCLUSTERED (currency_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_decline_reason
    ADD CONSTRAINT pk_dim_decline_reason PRIMARY KEY NONCLUSTERED (decline_reason_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_card_product
    ADD CONSTRAINT pk_dim_card_product PRIMARY KEY NONCLUSTERED (card_product_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_account
    ADD CONSTRAINT pk_dim_account PRIMARY KEY NONCLUSTERED (account_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_customer
    ADD CONSTRAINT pk_dim_customer PRIMARY KEY NONCLUSTERED (customer_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_merchant
    ADD CONSTRAINT pk_dim_merchant PRIMARY KEY NONCLUSTERED (merchant_sk) NOT ENFORCED;

ALTER TABLE dbo.dim_fx_rate
    ADD CONSTRAINT pk_dim_fx_rate PRIMARY KEY NONCLUSTERED (fx_rate_sk) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- Primary keys — facts and aggregate
-- -------------------------------------------------------------------------------------
-- The degenerate business key is the key, per the note at the top of 03_facts.sql. This is
-- also the merge key the idempotent load uses, so the PK and the load's notion of identity are
-- the same thing by construction rather than by coincidence — which is what stops a rerun from
-- doubling the fact table while the declared key still claims uniqueness.

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT pk_fact_transaction PRIMARY KEY NONCLUSTERED (transaction_id) NOT ENFORCED;

ALTER TABLE dbo.fact_dispute
    ADD CONSTRAINT pk_fact_dispute PRIMARY KEY NONCLUSTERED (dispute_id) NOT ENFORCED;

-- A composite key, because the grain *is* composite. Declaring it is what tells the optimiser
-- (and the semantic model) that this table has one row per merchant-day, which is the property
-- that makes it safe to sum.
ALTER TABLE dbo.agg_merchant_daily
    ADD CONSTRAINT pk_agg_merchant_daily
        PRIMARY KEY NONCLUSTERED (date_sk, merchant_sk) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- Alternate keys — the business keys the surrogates stand in for
-- -------------------------------------------------------------------------------------
-- These are the constraints that actually express the loaders' correctness conditions, and
-- they are the ones most worth having a test re-check. A duplicate here means either the silver
-- `unique` DQ rule let something through or the surrogate-key assignment ran twice against the
-- same batch — the second of which is precisely the rerun bug the `WHERE NOT EXISTS` anti-join
-- in each sp_load_dim_* exists to prevent.

ALTER TABLE dbo.dim_currency
    ADD CONSTRAINT uk_dim_currency UNIQUE NONCLUSTERED (currency_code) NOT ENFORCED;

ALTER TABLE dbo.dim_decline_reason
    ADD CONSTRAINT uk_dim_decline_reason
        UNIQUE NONCLUSTERED (decline_reason_code) NOT ENFORCED;

ALTER TABLE dbo.dim_card_product
    ADD CONSTRAINT uk_dim_card_product
        UNIQUE NONCLUSTERED (card_product_code) NOT ENFORCED;

-- SCD2: one version per key per effective instant. Not `(account_id)` — see above.
ALTER TABLE dbo.dim_account
    ADD CONSTRAINT uk_dim_account UNIQUE NONCLUSTERED (account_id, valid_from) NOT ENFORCED;

ALTER TABLE dbo.dim_customer
    ADD CONSTRAINT uk_dim_customer UNIQUE NONCLUSTERED (customer_id, valid_from) NOT ENFORCED;

ALTER TABLE dbo.dim_merchant
    ADD CONSTRAINT uk_dim_merchant UNIQUE NONCLUSTERED (merchant_id, valid_from) NOT ENFORCED;

-- One rate per currency pair per interval start. The same half-open-interval shape as SCD2,
-- for the same reason — see the FX note in 02_dimensions.sql.
ALTER TABLE dbo.dim_fx_rate
    ADD CONSTRAINT uk_dim_fx_rate
        UNIQUE NONCLUSTERED (from_currency, to_currency, valid_from_date) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- Foreign keys — fact_transaction
-- -------------------------------------------------------------------------------------
-- These reference the *surrogate* key of each dimension, which for the SCD2 dimensions means
-- they reference a specific version, not a business entity. That is the property that makes
-- point-in-time correctness hold once the fact is loaded: the version was chosen at load time
-- by comparing auth_ts to the dimension's validity interval, and the FK freezes that choice.
-- A model that joined on account_id instead would re-resolve the version at query time and
-- restate history.
--
-- Every one of these will resolve, including for unmatched source rows, because the loaders
-- COALESCE to the -1 unknown member rather than leaving NULL. That is what makes the FKs
-- non-nullable and therefore honest — a nullable FK on a star is usually a sign that the
-- unknown-member pattern was skipped.

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_date
        FOREIGN KEY (date_sk) REFERENCES dbo.dim_date (date_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_account
        FOREIGN KEY (account_sk) REFERENCES dbo.dim_account (account_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_customer
        FOREIGN KEY (customer_sk) REFERENCES dbo.dim_customer (customer_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_merchant
        FOREIGN KEY (merchant_sk) REFERENCES dbo.dim_merchant (merchant_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_card_product
        FOREIGN KEY (card_product_sk)
        REFERENCES dbo.dim_card_product (card_product_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_currency
        FOREIGN KEY (currency_sk) REFERENCES dbo.dim_currency (currency_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_transaction
    ADD CONSTRAINT fk_fact_transaction_decline_reason
        FOREIGN KEY (decline_reason_sk)
        REFERENCES dbo.dim_decline_reason (decline_reason_sk) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- Foreign keys — fact_dispute
-- -------------------------------------------------------------------------------------
-- Two date FKs to the same dimension, which is the textbook role-playing-dimension case. On
-- Fabric there are no views-as-aliases worth the maintenance here, so the semantic model does
-- the role-playing with two inactive relationships plus USERELATIONSHIP in the measures; these
-- declarations are what let it detect both.
--
-- There is deliberately **no FK from fact_dispute to fact_transaction**. transaction_id is
-- carried so the two can be related, but a fact-to-fact FK would assert that every dispute's
-- transaction is present in the warehouse, and that is exactly the condition the silver
-- `referential` DQ rule tests and quarantines on. Declaring it here would make the optimiser
-- trust a claim whose counterexamples the pipeline is built to expect.

ALTER TABLE dbo.fact_dispute
    ADD CONSTRAINT fk_fact_dispute_raised_date
        FOREIGN KEY (raised_date_sk) REFERENCES dbo.dim_date (date_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_dispute
    ADD CONSTRAINT fk_fact_dispute_resolved_date
        FOREIGN KEY (resolved_date_sk) REFERENCES dbo.dim_date (date_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_dispute
    ADD CONSTRAINT fk_fact_dispute_account
        FOREIGN KEY (account_sk) REFERENCES dbo.dim_account (account_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_dispute
    ADD CONSTRAINT fk_fact_dispute_merchant
        FOREIGN KEY (merchant_sk) REFERENCES dbo.dim_merchant (merchant_sk) NOT ENFORCED;

ALTER TABLE dbo.fact_dispute
    ADD CONSTRAINT fk_fact_dispute_currency
        FOREIGN KEY (currency_sk) REFERENCES dbo.dim_currency (currency_sk) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- Foreign keys — agg_merchant_daily
-- -------------------------------------------------------------------------------------

ALTER TABLE dbo.agg_merchant_daily
    ADD CONSTRAINT fk_agg_merchant_daily_date
        FOREIGN KEY (date_sk) REFERENCES dbo.dim_date (date_sk) NOT ENFORCED;

ALTER TABLE dbo.agg_merchant_daily
    ADD CONSTRAINT fk_agg_merchant_daily_merchant
        FOREIGN KEY (merchant_sk) REFERENCES dbo.dim_merchant (merchant_sk) NOT ENFORCED;

-- -------------------------------------------------------------------------------------
-- Statistics
-- -------------------------------------------------------------------------------------
-- Fabric creates statistics automatically, so this section is a small, deliberate supplement
-- rather than a wholesale manual regime: the columns below are the join and filter keys on the
-- largest table, and giving the optimiser a histogram on them before the first query is
-- cheaper than letting the first query pay for it.
--
-- One statistics object per column. Multi-column statistics are not supported (FB018), which
-- is worth knowing before writing `ON fact_transaction (date_sk, merchant_sk)` out of habit
-- from SQL Server.
CREATE STATISTICS stat_fact_transaction_date_sk
    ON dbo.fact_transaction (date_sk);

CREATE STATISTICS stat_fact_transaction_merchant_sk
    ON dbo.fact_transaction (merchant_sk);

CREATE STATISTICS stat_fact_transaction_account_sk
    ON dbo.fact_transaction (account_sk);

CREATE STATISTICS stat_fact_transaction_auth_ts
    ON dbo.fact_transaction (auth_ts);

CREATE STATISTICS stat_dim_account_account_id
    ON dbo.dim_account (account_id);

CREATE STATISTICS stat_dim_merchant_merchant_id
    ON dbo.dim_merchant (merchant_id);
