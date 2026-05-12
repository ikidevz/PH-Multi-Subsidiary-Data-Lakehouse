-- =============================================================================
-- 03_create_schemas.sql  [postgres-CENTRAL]
-- Purpose : Create the three medallion-architecture schemas inside the
--           'lakehouse' database.
--
--   bronze    (bronze) → data ingested as-is from CDC consumer
--   silver          → cleaned, deduplicated, validated by dbt
--   gold            → aggregated, business-ready marts for Superset
--
-- Runs on : postgres-central  |  connect to 'lakehouse' DB as superuser
--           (\c lakehouse  OR  psql -d lakehouse)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
