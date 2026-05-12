-- =============================================================================
-- 01_create_dept_databases.sql  [postgres-DEPT]
-- Purpose : Create all 24 department databases (8 depts × 3 subsidiaries).
--           WAL level is forced to logical via docker-compose CMD flags:
--             -c wal_level=logical
--             -c max_replication_slots=30
--             -c max_wal_senders=30
--           No ALTER SYSTEM needed — settings apply at container start.
--
-- NOTE: lakehouse / airflow / superset / datahub DBs are NOT here.
--       Those live on postgres-central (see init-scripts/central/).
--
-- Runs on : postgres-dept  |  connects to default 'postgres' DB as superuser
-- =============================================================================

-- ABC -------------------------------------------------------------------------
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_finance_abc')     THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_finance_abc');     END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_tax_abc')         THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_tax_abc');         END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_hr_abc')          THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_hr_abc');          END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_ops_abc')         THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_ops_abc');         END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_sales_abc')       THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_sales_abc');       END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_procurement_abc') THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_procurement_abc'); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_legal_abc')       THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_legal_abc');       END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_it_audit_abc')    THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_it_audit_abc');    END IF; END $$;

-- XYZ -------------------------------------------------------------------------
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_finance_xyz')     THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_finance_xyz');     END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_tax_xyz')         THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_tax_xyz');         END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_hr_xyz')          THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_hr_xyz');          END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_ops_xyz')         THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_ops_xyz');         END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_sales_xyz')       THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_sales_xyz');       END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_procurement_xyz') THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_procurement_xyz'); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_legal_xyz')       THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_legal_xyz');       END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_it_audit_xyz')    THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_it_audit_xyz');    END IF; END $$;

-- RTL -------------------------------------------------------------------------
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_finance_rtl')     THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_finance_rtl');     END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_tax_rtl')         THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_tax_rtl');         END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_hr_rtl')          THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_hr_rtl');          END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_ops_rtl')         THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_ops_rtl');         END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_sales_rtl')       THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_sales_rtl');       END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_procurement_rtl') THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_procurement_rtl'); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_legal_rtl')       THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_legal_rtl');       END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dept_it_audit_rtl')    THEN PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE dept_it_audit_rtl');    END IF; END $$;
