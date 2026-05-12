-- =============================================================================
-- 02_create_roles.sql  [postgres-CENTRAL]
-- Purpose : Create all application roles used by the lakehouse, Airflow,
--           Superset, and DataHub on postgres-central.
--
-- Roles created here are LOCAL to postgres-central.
-- Dept-scoped roles (abc_dept_user, etc.) live on postgres-dept.
--
-- Runs on : postgres-central  |  connects to default 'postgres' DB as superuser
-- =============================================================================

DO $$
BEGIN
    -- Read-only access for Superset / BI gold layer queries
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_reader') THEN
        CREATE ROLE superset_reader;
    END IF;

    -- Full DML access for the data pipeline (Airflow DAGs / cdc-consumer)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lakehouse_writer') THEN
        CREATE ROLE lakehouse_writer;
    END IF;

    -- Subsidiary-scoped read roles for silver/gold layer (row-level security)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'abc_user') THEN
        CREATE ROLE abc_user;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xyz_user') THEN
        CREATE ROLE xyz_user;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'retail_user') THEN
        CREATE ROLE retail_user;
    END IF;
END
$$;
