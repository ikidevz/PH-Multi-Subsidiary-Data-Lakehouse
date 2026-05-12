-- =============================================================================
-- 00_create_extensions.sql  [postgres-DEPT]
-- Purpose : Enable extensions needed by dept databases.
--           pgcrypto  → UUID generation in dept tables
--           dblink    → used by 01_create_databases.sql to CREATE DATABASE
--                       outside transaction blocks
-- Runs on : postgres-dept (default 'postgres' DB, superuser)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS dblink;
