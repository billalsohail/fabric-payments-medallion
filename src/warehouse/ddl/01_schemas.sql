-- =====================================================================================
-- wh_gold — 01: schemas
--
-- NOTE: this file, and every file under src/warehouse/, has never been executed by any
-- engine. It is parsed, subset-checked by tools/fabric_tsql_lint.py, and reviewed. The
-- *logic* it expresses is executed in Spark SQL by src/lib/gold.py and verified by
-- tests/test_gold_recon.py. See docs/gold-execution.md for why those are two different
-- claims and why neither substitutes for the other.
--
-- Three schemas, because they have three different lifecycles:
--
--   dbo  the star. Read by the semantic model. Changes only by deployment.
--   stg  landing zone for silver extracts inside the Warehouse. Truncated every load.
--   sec  security predicates and policies. Changes only by deployment, and separately
--        from dbo, because an RLS predicate function is schema-bound to the table it
--        filters: altering dbo.dim_account while sec.fn_account_region_filter binds it
--        fails. Keeping them in different schemas does not remove that coupling, but it
--        does make it visible in the deployment order.
--
-- Schema names must not contain '/' or '\' in Fabric Warehouse.
--   https://learn.microsoft.com/fabric/data-warehouse/tsql-surface-area (ms.date 2026-08-26)
-- =====================================================================================

CREATE SCHEMA stg;

CREATE SCHEMA sec;

-- dbo is created with the Warehouse, so it is deliberately not created here. Attempting to
-- would fail the whole deployment batch on a fresh Warehouse, which is the opposite of
-- idempotent.
