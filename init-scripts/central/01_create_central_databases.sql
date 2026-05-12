-- =============================================================================
-- 01_create_central_databases.sql  [postgres-CENTRAL]
-- Purpose : Create all platform databases on the central instance.
--           'lakehouse' is also set as POSTGRES_DB in docker-compose so it is
--           auto-created by the image entrypoint — the DO block below is a
--           safe no-op if it already exists.
--
-- Databases:
--   lakehouse  → medallion warehouse (bronze / silver / gold schemas)
--   airflow    → Airflow task state, DAG runs, connections metadata
--   superset   → Superset dashboards, charts, user sessions metadata
--   datahub    → DataHub GMS catalog entities and lineage metadata
--
-- NOTE: Dept databases (dept_*_abc/xyz/rtl) are NOT here.
--       Those live on postgres-dept (see init-scripts/dept/).
--
-- Runs on : postgres-central  |  connects to default 'postgres' DB as superuser
-- =============================================================================

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'lakehouse') THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE lakehouse'); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')   THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE airflow');   END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'superset')  THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE superset');  END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datahub')   THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE datahub');   END IF; END $$;
