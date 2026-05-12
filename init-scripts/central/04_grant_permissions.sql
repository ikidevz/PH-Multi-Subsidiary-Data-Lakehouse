-- =============================================================================
-- 04_grant_permissions.sql  [postgres-CENTRAL]
-- Purpose : Grant database-level, schema-level, table-level, and default
--           privileges to all application roles on the lakehouse database.
--
-- Runs on : postgres-central  |  connect to 'lakehouse' DB as superuser
-- =============================================================================

-- Database-level: allow all roles to connect to lakehouse
GRANT CONNECT ON DATABASE lakehouse
    TO superset_reader, lakehouse_writer, abc_user, xyz_user, retail_user;

-- Schema-level: allow all roles to see objects inside each schema
GRANT USAGE ON SCHEMA bronze, silver, gold
    TO superset_reader, lakehouse_writer, abc_user, xyz_user, retail_user;

-- Table-level on EXISTING tables -----------------------------------------------

-- superset_reader: read-only across all layers
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, silver, gold
    TO superset_reader;

-- lakehouse_writer: full DML across all layers (Airflow DAGs + cdc-consumer)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze, silver, gold
    TO lakehouse_writer;

-- Subsidiary-scoped roles: read silver & gold only (no bronze access)
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold TO abc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold TO xyz_user;
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold TO retail_user;

-- Default privileges: automatically apply to FUTURE tables ----------------------
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold
    GRANT SELECT ON TABLES TO superset_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lakehouse_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver, gold
    GRANT SELECT ON TABLES TO abc_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver, gold
    GRANT SELECT ON TABLES TO xyz_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver, gold
    GRANT SELECT ON TABLES TO retail_user;

-- Sequence-level: lakehouse_writer needs USAGE on sequences for SERIAL PKs
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze, silver, gold
    TO lakehouse_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold
    GRANT USAGE, SELECT ON SEQUENCES TO lakehouse_writer;
