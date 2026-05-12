-- =============================================================================
-- 02_create_roles.sql  [postgres-DEPT]
-- Purpose : Create application roles scoped to the dept OLTP instance.
--           These roles control which subsidiary's dept service can access
--           which databases. Debezium uses the superuser (postgres) for WAL.
--
-- Roles created here are LOCAL to postgres-dept.
-- lakehouse_writer / superset_reader / etc. live on postgres-central.
--
-- Runs on : postgres-dept  |  connects to default 'postgres' DB as superuser
-- =============================================================================

DO $$
BEGIN
    -- Per-subsidiary dept access roles
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'abc_dept_user') THEN
        CREATE ROLE abc_dept_user LOGIN PASSWORD 'change_me_abc';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xyz_dept_user') THEN
        CREATE ROLE xyz_dept_user LOGIN PASSWORD 'change_me_xyz';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rtl_dept_user') THEN
        CREATE ROLE rtl_dept_user LOGIN PASSWORD 'change_me_rtl';
    END IF;

    -- Debezium replication role (needs REPLICATION + LOGIN)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_user') THEN
        CREATE ROLE debezium_user LOGIN REPLICATION PASSWORD 'change_me_debezium';
    END IF;
END
$$;

-- Grant each subsidiary role connect rights to its own 8 databases
-- ABC
GRANT CONNECT ON DATABASE dept_finance_abc     TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_tax_abc         TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_hr_abc          TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_ops_abc         TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_sales_abc       TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_procurement_abc TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_legal_abc       TO abc_dept_user;
GRANT CONNECT ON DATABASE dept_it_audit_abc    TO abc_dept_user;

-- XYZ
GRANT CONNECT ON DATABASE dept_finance_xyz     TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_tax_xyz         TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_hr_xyz          TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_ops_xyz         TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_sales_xyz       TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_procurement_xyz TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_legal_xyz       TO xyz_dept_user;
GRANT CONNECT ON DATABASE dept_it_audit_xyz    TO xyz_dept_user;

-- RTL
GRANT CONNECT ON DATABASE dept_finance_rtl     TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_tax_rtl         TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_hr_rtl          TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_ops_rtl         TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_sales_rtl       TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_procurement_rtl TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_legal_rtl       TO rtl_dept_user;
GRANT CONNECT ON DATABASE dept_it_audit_rtl    TO rtl_dept_user;
