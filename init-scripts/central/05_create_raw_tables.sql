-- =============================================================================
-- 05_create_bronze_tables.sql
-- Purpose : Create all tables in the bronze (bronze) schema.
--           Tables are organised by domain: meta → finance → hr → ops →
--           tax → legal / compliance → audit.
-- Runs on : postgres-central  |  connect to 'lakehouse' DB as superuser
-- =============================================================================

-- ---------------------------------------------------------------------------
-- META / PIPELINE
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.subsidiaries (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL UNIQUE,
    name                VARCHAR(255)    NOT NULL,
    tin                 VARCHAR(20)     NOT NULL UNIQUE,
    sec_reg             VARCHAR(50),
    bir_rdo             VARCHAR(10),
    industry            VARCHAR(100),
    scale_factor        NUMERIC(5,2)    NOT NULL DEFAULT 1.0,
    fiscal_year_start   INTEGER         NOT NULL DEFAULT 1,  -- 1 = January
    currency            VARCHAR(5)      NOT NULL DEFAULT 'PHP',
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.pipeline_run_log (
    id                  SERIAL          PRIMARY KEY,
    run_id              VARCHAR(100)    NOT NULL,
    dag_id              VARCHAR(100),
    task_id             VARCHAR(100),
    subsidiary_id       VARCHAR(10),
    source_dept         VARCHAR(50),
    endpoint_called     TEXT,
    rows_fetched        INTEGER         DEFAULT 0,
    rows_inserted       INTEGER         DEFAULT 0,
    rows_rejected       INTEGER         DEFAULT 0,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running',
    error_message       TEXT,
    started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMP,
    duration_seconds    NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS bronze.cdc_event_log (
    id                  SERIAL          PRIMARY KEY,
    kafka_topic         VARCHAR(255)    NOT NULL,
    kafka_partition     INTEGER         NOT NULL,
    kafka_offset        BIGINT          NOT NULL,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50),
    source_table        VARCHAR(100)    NOT NULL,
    operation           VARCHAR(20)     NOT NULL,   -- INSERT | UPDATE | DELETE
    record_id           VARCHAR(255),
    before_state        JSONB,
    after_state         JSONB,
    event_ts            TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    consumed_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    is_processed        BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_cdc_event UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

-- ---------------------------------------------------------------------------
-- FINANCE
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.journal_entries (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'finance',
    entry_date          DATE            NOT NULL,
    entry_number        VARCHAR(100)    NOT NULL,
    account_code        VARCHAR(20)     NOT NULL,
    account_name        VARCHAR(255),
    debit_amount        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    credit_amount       NUMERIC(18,2)   NOT NULL DEFAULT 0,
    description         TEXT,
    reference_doc       VARCHAR(100),
    is_interco          BOOLEAN         NOT NULL DEFAULT FALSE,
    counterpart_sub_id  VARCHAR(10),
    cost_center         VARCHAR(50),
    posted_by           VARCHAR(100),
    approved_by         VARCHAR(100),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_je_number UNIQUE (subsidiary_id, entry_number)
);

CREATE TABLE IF NOT EXISTS bronze.ap_invoices (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'finance',
    invoice_number      VARCHAR(100)    NOT NULL,
    vendor_id           VARCHAR(50),
    vendor_name         VARCHAR(255),
    vendor_tin          VARCHAR(20),
    invoice_date        DATE            NOT NULL,
    due_date            DATE,
    gross_amount        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    vat_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    net_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    wht_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    payment_amount      NUMERIC(18,2),
    payment_date        DATE,
    status              VARCHAR(20)     NOT NULL DEFAULT 'unpaid',
    is_interco          BOOLEAN         NOT NULL DEFAULT FALSE,
    counterpart_sub_id  VARCHAR(10),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ap_invoice UNIQUE (subsidiary_id, invoice_number)
);

CREATE TABLE IF NOT EXISTS bronze.ar_ledger (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'finance',
    invoice_number      VARCHAR(100)    NOT NULL,
    customer_id         VARCHAR(50),
    customer_name       VARCHAR(255),
    customer_tin        VARCHAR(20),
    invoice_date        DATE            NOT NULL,
    due_date            DATE,
    gross_amount        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    vat_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    net_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    collected_amount    NUMERIC(18,2),
    collection_date     DATE,
    status              VARCHAR(20)     NOT NULL DEFAULT 'open',
    is_interco          BOOLEAN         NOT NULL DEFAULT FALSE,
    counterpart_sub_id  VARCHAR(10),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ar_invoice UNIQUE (subsidiary_id, invoice_number)
);

-- ---------------------------------------------------------------------------
-- SALES
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.sales_transactions (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'sales',
    invoice_number      VARCHAR(100)    NOT NULL,
    customer_name       VARCHAR(255),
    transaction_date    DATE            NOT NULL,
    gross_amount        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    discount_amount     NUMERIC(18,2)   NOT NULL DEFAULT 0,
    vat_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    net_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    vat_classification  VARCHAR(50),
    is_interco          BOOLEAN         NOT NULL DEFAULT FALSE,
    counterpart_sub_id  VARCHAR(10),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_sales_invoice UNIQUE (subsidiary_id, invoice_number)
);

CREATE TABLE IF NOT EXISTS bronze.customers (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'sales',
    customer_id         VARCHAR(50)     NOT NULL,
    customer_name       VARCHAR(255),
    customer_tin        VARCHAR(20),
    contact_email       VARCHAR(255),
    phone               VARCHAR(50),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_customer_id UNIQUE (subsidiary_id, customer_id)
);

CREATE TABLE IF NOT EXISTS bronze.campaigns (
    id                      SERIAL          PRIMARY KEY,
    subsidiary_id           VARCHAR(10)     NOT NULL,
    source_dept             VARCHAR(50)     NOT NULL DEFAULT 'sales',
    campaign_id             VARCHAR(100)    NOT NULL,
    campaign_name           VARCHAR(255)    NOT NULL,
    campaign_type           VARCHAR(50)     NOT NULL DEFAULT 'digital',
    channel                 VARCHAR(50),
    start_date              DATE            NOT NULL,
    end_date                DATE,
    budget_amount           NUMERIC(18,2)   NOT NULL DEFAULT 0,
    actual_spend            NUMERIC(18,2)   NOT NULL DEFAULT 0,
    leads_generated         INTEGER         NOT NULL DEFAULT 0,
    conversions             INTEGER         NOT NULL DEFAULT 0,
    revenue_attributed      NUMERIC(18,2)   NOT NULL DEFAULT 0,
    status                  VARCHAR(20)     NOT NULL DEFAULT 'active',
    ingested_at             TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_campaign           UNIQUE (subsidiary_id, campaign_id),
    CONSTRAINT chk_campaign_type     CHECK (campaign_type IN ('digital','print','events','radio','tv','referral')),
    CONSTRAINT chk_campaign_status   CHECK (status IN ('planned','active','completed','cancelled')),
    CONSTRAINT chk_campaign_dates    CHECK (end_date IS NULL OR end_date >= start_date)
);

-- ---------------------------------------------------------------------------
-- HR / PAYROLL
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.employees (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'hr',
    employee_id         VARCHAR(50)     NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    tin                 VARCHAR(20),
    sss_number          VARCHAR(20),
    philhealth_number   VARCHAR(20),
    pagibig_number      VARCHAR(20),
    department          VARCHAR(100),
    position            VARCHAR(100),
    employment_type     VARCHAR(30),
    date_hired          DATE,
    basic_salary        NUMERIC(18,2),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_employee_id UNIQUE (subsidiary_id, employee_id)
);

CREATE TABLE IF NOT EXISTS bronze.payroll_runs (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'hr',
    payroll_period      VARCHAR(20)     NOT NULL,
    employee_id         VARCHAR(50)     NOT NULL,
    gross_pay           NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_deductions    NUMERIC(18,2)   NOT NULL DEFAULT 0,
    net_pay             NUMERIC(18,2)   NOT NULL DEFAULT 0,
    payroll_date        DATE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.statutory_contributions (
    id                      SERIAL          PRIMARY KEY,
    subsidiary_id           VARCHAR(10)     NOT NULL,
    source_dept             VARCHAR(50)     NOT NULL DEFAULT 'hr',
    period_month            INTEGER         NOT NULL,
    period_year             INTEGER         NOT NULL,
    sss_employer            NUMERIC(18,2)   NOT NULL DEFAULT 0,
    sss_employee            NUMERIC(18,2)   NOT NULL DEFAULT 0,
    philhealth_employer     NUMERIC(18,2)   NOT NULL DEFAULT 0,
    philhealth_employee     NUMERIC(18,2)   NOT NULL DEFAULT 0,
    pagibig_employer        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    pagibig_employee        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    total_amount            NUMERIC(18,2)   NOT NULL DEFAULT 0,
    ingested_at             TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- OPERATIONS / PROCUREMENT
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.vendors (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'procurement',
    vendor_id           VARCHAR(50)     NOT NULL,
    vendor_name         VARCHAR(255),
    vendor_tin          VARCHAR(20),
    vendor_type         VARCHAR(50),
    payment_terms       VARCHAR(50),
    is_accredited       BOOLEAN         NOT NULL DEFAULT FALSE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_vendor_id UNIQUE (subsidiary_id, vendor_id)
);

CREATE TABLE IF NOT EXISTS bronze.purchase_orders (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'ops',
    po_number           VARCHAR(100)    NOT NULL,
    vendor_id           VARCHAR(50),
    order_date          DATE,
    expected_date       DATE,
    status              VARCHAR(30),
    total_amount        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_po_number UNIQUE (subsidiary_id, po_number)
);

CREATE TABLE IF NOT EXISTS bronze.sales_orders (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'ops',
    so_number           VARCHAR(100)    NOT NULL,
    customer_id         VARCHAR(50),
    order_date          DATE,
    ship_date           DATE,
    status              VARCHAR(30),
    total_amount        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_so_number UNIQUE (subsidiary_id, so_number)
);

CREATE TABLE IF NOT EXISTS bronze.inventory_movements (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'ops',
    movement_date       DATE            NOT NULL,
    movement_type       VARCHAR(50)     NOT NULL,
    item_code           VARCHAR(100),
    quantity            INTEGER,
    unit_cost           NUMERIC(18,2),
    total_cost          NUMERIC(18,2),
    warehouse_location  VARCHAR(100),
    reference_doc       VARCHAR(100),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- TAX
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.vat_returns (
    id                      SERIAL          PRIMARY KEY,
    subsidiary_id           VARCHAR(10)     NOT NULL,
    source_dept             VARCHAR(50)     NOT NULL DEFAULT 'tax',
    period_month            INTEGER         NOT NULL,
    period_year             INTEGER         NOT NULL,
    form_type               VARCHAR(20)     NOT NULL,
    gross_sales             NUMERIC(18,2)   NOT NULL DEFAULT 0,
    exempt_sales            NUMERIC(18,2)   NOT NULL DEFAULT 0,
    zero_rated_sales        NUMERIC(18,2)   NOT NULL DEFAULT 0,
    taxable_sales           NUMERIC(18,2)   NOT NULL DEFAULT 0,
    output_vat              NUMERIC(18,2)   NOT NULL DEFAULT 0,
    input_vat               NUMERIC(18,2)   NOT NULL DEFAULT 0,
    vat_payable             NUMERIC(18,2)   NOT NULL DEFAULT 0,
    filing_date             DATE,
    efps_confirmation       VARCHAR(100),
    status                  VARCHAR(20)     NOT NULL DEFAULT 'pending',
    ingested_at             TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_vat_period UNIQUE (subsidiary_id, period_year, period_month, form_type)
);

CREATE TABLE IF NOT EXISTS bronze.wht_filings (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'tax',
    form_type           VARCHAR(20)     NOT NULL,
    period_month        INTEGER,
    period_year         INTEGER,
    payee_tin           VARCHAR(20),
    payee_name          VARCHAR(255),
    income_payment      NUMERIC(18,2)   NOT NULL DEFAULT 0,
    atc_code            VARCHAR(10),
    tax_rate            NUMERIC(6,4)    NOT NULL DEFAULT 0,
    wht_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    filing_date         DATE,
    efps_confirmation   VARCHAR(100),
    status              VARCHAR(20)     NOT NULL DEFAULT 'pending',
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.wht_certificates (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'procurement',
    cert_number         VARCHAR(100)    NOT NULL,
    vendor_id           VARCHAR(50),
    atc_code            VARCHAR(10),
    income_payment      NUMERIC(18,2)   NOT NULL DEFAULT 0,
    tax_rate            NUMERIC(6,4)    NOT NULL DEFAULT 0,
    wht_amount          NUMERIC(18,2)   NOT NULL DEFAULT 0,
    issued_date         DATE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_cert_number UNIQUE (subsidiary_id, cert_number)
);

CREATE TABLE IF NOT EXISTS bronze.bir_filings_log (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'tax',
    form_type           VARCHAR(20)     NOT NULL,
    period_covered      VARCHAR(20),
    due_date            DATE,
    filing_date         DATE,
    amount_paid         NUMERIC(18,2)   NOT NULL DEFAULT 0,
    surcharge_amount    NUMERIC(18,2)   NOT NULL DEFAULT 0,
    interest_amount     NUMERIC(18,2)   NOT NULL DEFAULT 0,
    compromise_amount   NUMERIC(18,2)   NOT NULL DEFAULT 0,
    -- Generated column: total penalty = surcharge + interest + compromise
    penalty_amount      NUMERIC(18,2)   GENERATED ALWAYS AS (
                            surcharge_amount + interest_amount + compromise_amount
                        ) STORED,
    efps_ref            VARCHAR(100),
    payment_mode        VARCHAR(30),
    status              VARCHAR(30)     NOT NULL DEFAULT 'filed',
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- LEGAL / CORPORATE COMPLIANCE
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.sec_filings (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'legal',
    filing_type         VARCHAR(50)     NOT NULL,
    due_date            DATE,
    filing_date         DATE,
    status              VARCHAR(50),
    reference_number    VARCHAR(100),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.stockholders (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    stockholder_id      VARCHAR(50)     NOT NULL,
    name                VARCHAR(255),
    ownership_pct       NUMERIC(6,2),
    is_foreign          BOOLEAN         NOT NULL DEFAULT FALSE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_stockholder UNIQUE (subsidiary_id, stockholder_id)
);

CREATE TABLE IF NOT EXISTS bronze.board_resolutions (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    resolution_number   VARCHAR(100)    NOT NULL,
    resolution_date     DATE,
    title               VARCHAR(255),
    enacted_by          VARCHAR(255),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_resolution UNIQUE (subsidiary_id, resolution_number)
);

CREATE TABLE IF NOT EXISTS bronze.officers (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    officer_id          VARCHAR(50)     NOT NULL,
    name                VARCHAR(255),
    position            VARCHAR(100),
    start_date          DATE,
    end_date            DATE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_officer UNIQUE (subsidiary_id, officer_id)
);

-- ---------------------------------------------------------------------------
-- IT AUDIT / SECURITY
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.access_events (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'it_audit',
    event_timestamp     TIMESTAMP       NOT NULL,
    user_id             VARCHAR(50),
    action              VARCHAR(50),
    resource            VARCHAR(100),
    resource_id         VARCHAR(100),
    ip_address          VARCHAR(50),
    result              VARCHAR(50),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.audit_log (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    source_dept         VARCHAR(50)     NOT NULL DEFAULT 'it_audit',
    event_timestamp     TIMESTAMP       NOT NULL,
    user_id             VARCHAR(50),
    action              VARCHAR(50),
    result              VARCHAR(50),
    risk_level          VARCHAR(50),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.system_incidents (
    id                  SERIAL          PRIMARY KEY,
    subsidiary_id       VARCHAR(10)     NOT NULL,
    incident_id         VARCHAR(100)    NOT NULL,
    incident_date       DATE,
    incident_type       VARCHAR(100),
    severity            VARCHAR(50),
    description         TEXT,
    resolved_date       DATE,
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_incident UNIQUE (subsidiary_id, incident_id)
);
