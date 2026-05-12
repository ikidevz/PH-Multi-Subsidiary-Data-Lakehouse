-- =============================================================================
-- 00_create_extensions.sql  [postgres-CENTRAL]
-- Purpose : Enable extensions needed on the central lakehouse instance.
--           pgcrypto → mask_tin() SHA-256 hashing in dbt macros
--           dblink   → lets 01_create_central_databases.sql run CREATE DATABASE
--                      outside a transaction block
-- Runs on : postgres-central  |  connects to default 'postgres' DB as superuser
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS dblink;
