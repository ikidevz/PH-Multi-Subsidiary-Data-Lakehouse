-- =============================================================================
-- 08_seed_data.sql
-- Purpose : Insert initial reference / seed data.
--           Uses ON CONFLICT DO NOTHING so it is safe to re-run.
-- Database: connect as superuser (or lakehouse_writer) to 'lakehouse'
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Subsidiaries
-- ---------------------------------------------------------------------------

INSERT INTO bronze.subsidiaries
    (subsidiary_id, name, tin, sec_reg, bir_rdo, industry, scale_factor, fiscal_year_start)
VALUES
    ('ABC', 'ABC Trading Inc.',        '123-456-789-000', 'CS201900001', '040', 'Trading',       1.00, 1),
    ('XYZ', 'XYZ Manufacturing Corp.', '987-654-321-000', 'CS202000002', '041', 'Manufacturing', 1.10, 1),
    ('RTL', 'PH Retail Co.',           '456-123-789-000', 'CS202100088', '044', 'Retail',        0.75, 1)
ON CONFLICT (subsidiary_id) DO NOTHING;
