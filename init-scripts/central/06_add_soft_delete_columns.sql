-- =============================================================================
-- 05b_add_soft_delete_columns.sql
-- Purpose : Add is_deleted / deleted_at audit columns to every bronze.* table
--           so the CDC consumer's soft-delete UPDATE path does not fail.
--
-- FIX for Critical Bug 3: cdc_consumer.py executes
--     UPDATE {bronze_table} SET is_deleted = TRUE, deleted_at = NOW() ...
-- but 05_create_bronze_tables.sql never defined those columns, causing every
-- CDC DELETE event to roll back with a "column does not exist" error.
--
-- Run this script AFTER 05_create_bronze_tables.sql.
-- All ADD COLUMN calls are safe to re-run (IF NOT EXISTS guard).
-- Database: connect as superuser (or lakehouse_writer) to 'lakehouse'
-- =============================================================================

DO $$
DECLARE
    tbl TEXT;
    tbls TEXT[] := ARRAY[
        'bronze.subsidiaries',
        'bronze.pipeline_run_log',
        'bronze.cdc_event_log',
        'bronze.journal_entries',
        'bronze.ap_invoices',
        'bronze.ar_ledger',
        'bronze.sales_transactions',
        'bronze.customers',
        'bronze.campaigns',
        'bronze.employees',
        'bronze.payroll_runs',
        'bronze.statutory_contributions',
        'bronze.vendors',
        'bronze.purchase_orders',
        'bronze.sales_orders',
        'bronze.inventory_movements',
        'bronze.vat_returns',
        'bronze.wht_filings',
        'bronze.wht_certificates',
        'bronze.bir_filings_log',
        'bronze.sec_filings',
        'bronze.stockholders',
        'bronze.board_resolutions',
        'bronze.officers',
        'bronze.access_events',
        'bronze.audit_log',
        'bronze.system_incidents'
    ];
BEGIN
    FOREACH tbl IN ARRAY tbls LOOP
        -- is_deleted: marks the row as CDC-deleted without a physical DELETE
        EXECUTE format(
            'ALTER TABLE %s ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE',
            tbl
        );
        -- deleted_at: timestamp set when the CDC consumer processes a DELETE event
        EXECUTE format(
            'ALTER TABLE %s ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP DEFAULT NULL',
            tbl
        );
    END LOOP;
END
$$;

-- Index on is_deleted so dbt silver queries can efficiently filter out
-- soft-deleted rows without a full sequential scan.
CREATE INDEX IF NOT EXISTS idx_journal_entries_not_deleted
    ON bronze.journal_entries (subsidiary_id) WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_sales_transactions_not_deleted
    ON bronze.sales_transactions (subsidiary_id) WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_employees_not_deleted
    ON bronze.employees (subsidiary_id) WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_ap_invoices_not_deleted
    ON bronze.ap_invoices (subsidiary_id) WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_ar_ledger_not_deleted
    ON bronze.ar_ledger (subsidiary_id) WHERE is_deleted = FALSE;

-- NOTE: dbt silver models should add WHERE is_deleted = FALSE
-- to all staging queries to exclude CDC-deleted rows from the silver layer.