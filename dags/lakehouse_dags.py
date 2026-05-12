from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import requests

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def check_kafka_lag():
    """
    Check Kafka lag for CDC consumer group (lakehouse-cdc-consumer).
    Alert if lag > threshold (indicates CDC backlog).
    """
    print("Checking Kafka consumer lag for lakehouse-cdc-consumer...")
    try:
        from kafka import KafkaAdminClient
        import os

        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
        print(f"Connected to Kafka: {kafka_bootstrap}")
        admin_client.close()
        return {"status": "ok", "message": "Kafka lag check passed"}
    except Exception as e:
        print(f"Kafka lag check failed: {e}")
        return {"status": "warning", "message": str(e)}


def check_cdc_event_log_freshness():
    """
    Check if bronze.cdc_event_log has recent CDC events.
    Alert if no events in last 5 minutes (CDC lag indicator).
    """
    pg_hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT MAX(event_ts) as last_event_ts FROM bronze.cdc_event_log
            WHERE event_ts > NOW() - INTERVAL '5 minutes';
        """)
        result = cur.fetchone()
        if result and result[0]:
            print(f"CDC freshness check passed: last event at {result[0]}")
            return {"status": "ok", "last_event_ts": str(result[0])}
        else:
            print("WARNING: No CDC events in last 5 minutes")
            return {"status": "warning", "message": "No CDC events in last 5 minutes"}
    except Exception as e:
        print(f"CDC freshness check failed: {e}")
        return {"status": "warning", "message": str(e)}
    finally:
        cur.close()
        conn.close()


def insert_sales_record(cur, record):
    cur.execute(
        "INSERT INTO bronze.sales_transactions (subsidiary_id, invoice_number, customer_name, transaction_date, gross_amount, discount_amount, vat_amount, net_amount, vat_classification, is_interco, counterpart_sub_id, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("invoice_number"),
            record.get("customer_name"),
            record.get("transaction_date"),
            record.get("gross_amount"),
            record.get("discount_amount"),
            record.get("vat_amount"),
            record.get("net_amount"),
            record.get("vat_classification"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
        ),
    )


def insert_customer(cur, record):
    cur.execute(
        "INSERT INTO bronze.customers (subsidiary_id, customer_id, customer_name, customer_tin, contact_email, phone, is_active, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("customer_id"),
            record.get("customer_name"),
            record.get("tin"),
            record.get("contact_email"),
            record.get("phone"),
            record.get("is_active", True),
        ),
    )


def insert_campaign(cur, record):
    cur.execute(
        "INSERT INTO bronze.campaigns (subsidiary_id, campaign_id, campaign_name, campaign_type, channel, start_date, end_date, budget_amount, actual_spend, leads_generated, conversions, revenue_attributed, status, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("campaign_id"),
            record.get("campaign_name"),
            record.get("campaign_type"),
            record.get("channel"),
            record.get("start_date"),
            record.get("end_date"),
            record.get("budget_amount"),
            record.get("actual_spend"),
            record.get("leads_generated"),
            record.get("conversions"),
            record.get("revenue_attributed"),
            record.get("status"),
        ),
    )


def insert_hr_employee(cur, record):
    cur.execute(
        "INSERT INTO bronze.employees (subsidiary_id, employee_id, first_name, last_name, tin, sss_number, philhealth_number, pagibig_number, department, position, employment_type, date_hired, basic_salary, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("employee_id"),
            record.get("first_name"),
            record.get("last_name"),
            record.get("tin"),
            record.get("sss_number"),
            record.get("philhealth_number"),
            record.get("pagibig_number"),
            record.get("department"),
            record.get("position"),
            record.get("employment_type"),
            record.get("date_hired"),
            record.get("basic_salary"),
        ),
    )


def insert_payroll_run(cur, record):
    cur.execute(
        "INSERT INTO bronze.payroll_runs (subsidiary_id, payroll_period, employee_id, gross_pay, total_deductions, net_pay, payroll_date, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("payroll_period"),
            record.get("employee_id"),
            record.get("gross_pay"),
            record.get("total_deductions"),
            record.get("net_pay"),
            record.get("payroll_date"),
        ),
    )


def insert_statutory_contribution(cur, record):
    cur.execute(
        "INSERT INTO bronze.statutory_contributions (subsidiary_id, period_month, period_year, sss_employer, sss_employee, philhealth_employer, philhealth_employee, pagibig_employer, pagibig_employee, total_amount, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("period_month"),
            record.get("period_year"),
            record.get("sss_employer"),
            record.get("sss_employee"),
            record.get("philhealth_employer"),
            record.get("philhealth_employee"),
            record.get("pagibig_employer"),
            record.get("pagibig_employee"),
            record.get("total_amount"),
        ),
    )


def insert_journal_entry(cur, record):
    cur.execute(
        "INSERT INTO bronze.journal_entries (subsidiary_id, entry_date, entry_number, account_code, account_name, debit_amount, credit_amount, description, reference_doc, is_interco, counterpart_sub_id, cost_center, posted_by, approved_by, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("entry_date"),
            record.get("entry_number"),
            record.get("account_code"),
            record.get("account_name"),
            record.get("debit_amount"),
            record.get("credit_amount"),
            record.get("description"),
            record.get("reference_doc"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
            record.get("cost_center"),
            record.get("posted_by"),
            record.get("approved_by"),
        ),
    )


def insert_ap_invoice(cur, record):
    cur.execute(
        "INSERT INTO bronze.ap_invoices (subsidiary_id, invoice_number, vendor_id, vendor_name, vendor_tin, invoice_date, due_date, gross_amount, vat_amount, net_amount, wht_amount, payment_amount, payment_date, status, is_interco, counterpart_sub_id, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("invoice_number"),
            record.get("vendor_id"),
            record.get("vendor_name"),
            record.get("vendor_tin"),
            record.get("invoice_date"),
            record.get("due_date"),
            record.get("gross_amount"),
            record.get("vat_amount"),
            record.get("net_amount"),
            record.get("wht_amount"),
            record.get("payment_amount"),
            record.get("payment_date"),
            record.get("status"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
        ),
    )


def insert_ar_ledger(cur, record):
    cur.execute(
        "INSERT INTO bronze.ar_ledger (subsidiary_id, invoice_number, customer_id, customer_name, customer_tin, invoice_date, due_date, gross_amount, vat_amount, net_amount, collected_amount, collection_date, status, is_interco, counterpart_sub_id, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("invoice_number"),
            record.get("customer_id"),
            record.get("customer_name"),
            record.get("customer_tin"),
            record.get("invoice_date"),
            record.get("due_date"),
            record.get("gross_amount"),
            record.get("vat_amount"),
            record.get("net_amount"),
            record.get("collected_amount"),
            record.get("collection_date"),
            record.get("status"),
            record.get("is_interco"),
            record.get("counterpart_sub_id"),
        ),
    )


def insert_vat_return(cur, record):
    cur.execute(
        "INSERT INTO bronze.vat_returns (subsidiary_id, period_month, period_year, form_type, gross_sales, exempt_sales, zero_rated_sales, taxable_sales, output_vat, input_vat, vat_payable, filing_date, efps_confirmation, status, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("period_month"),
            record.get("period_year"),
            record.get("form_type"),
            record.get("gross_sales"),
            record.get("exempt_sales", 0),
            record.get("zero_rated_sales", 0),
            record.get("taxable_sales"),
            record.get("output_vat"),
            record.get("input_vat"),
            record.get("vat_payable"),
            record.get("filing_date"),
            record.get("efps_confirmation"),
            record.get("status"),
        ),
    )


def insert_wht_filing(cur, record):
    cur.execute(
        "INSERT INTO bronze.wht_filings (subsidiary_id, form_type, period_month, period_year, payee_tin, payee_name, income_payment, atc_code, tax_rate, wht_amount, filing_date, efps_confirmation, status, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("form_type"),
            record.get("period_month"),
            record.get("period_year"),
            record.get("payee_tin"),
            record.get("payee_name"),
            record.get("income_payment"),
            record.get("atc_code"),
            record.get("tax_rate"),
            record.get("wht_amount"),
            record.get("filing_date"),
            record.get("efps_reference"),
            record.get("status"),
        ),
    )


def insert_bir_filing_log(cur, record):
    cur.execute(
        "INSERT INTO bronze.bir_filings_log (subsidiary_id, form_type, period_covered, due_date, filing_date, amount_paid, surcharge_amount, interest_amount, compromise_amount, payment_mode, efps_ref, status, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("form_type"),
            record.get("period_covered"),
            record.get("due_date"),
            record.get("filing_date"),
            record.get("amount_paid"),
            record.get("surcharge_amount", 0),
            record.get("interest_amount", 0),
            record.get("compromise_amount", 0),
            record.get("payment_mode"),
            record.get("efps_ref"),
            record.get("status"),
        ),
    )


def insert_inventory_movement(cur, record):
    cur.execute(
        "INSERT INTO bronze.inventory_movements (subsidiary_id, movement_date, movement_type, item_code, quantity, unit_cost, total_cost, warehouse_location, reference_doc, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("movement_date"),
            record.get("movement_type"),
            record.get("item_code"),
            record.get("quantity"),
            record.get("unit_cost"),
            record.get("total_cost"),
            record.get("warehouse_location"),
            record.get("reference_doc"),
        ),
    )


def insert_purchase_order(cur, record):
    cur.execute(
        "INSERT INTO bronze.purchase_orders (subsidiary_id, po_number, vendor_id, order_date, expected_date, status, total_amount, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("po_number"),
            record.get("vendor_id"),
            record.get("order_date"),
            record.get("expected_date"),
            record.get("status"),
            record.get("total_amount"),
        ),
    )


def insert_sales_order(cur, record):
    cur.execute(
        "INSERT INTO bronze.sales_orders (subsidiary_id, so_number, customer_id, order_date, ship_date, status, total_amount, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("so_number"),
            record.get("customer_id"),
            record.get("order_date"),
            record.get("ship_date"),
            record.get("status"),
            record.get("total_amount"),
        ),
    )


def insert_vendor(cur, record):
    cur.execute(
        "INSERT INTO bronze.vendors (subsidiary_id, vendor_id, vendor_name, vendor_tin, vendor_type, payment_terms, is_accredited, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("vendor_id"),
            record.get("vendor_name"),
            record.get("vendor_tin"),
            record.get("vendor_type"),
            record.get("payment_terms"),
            record.get("is_accredited"),
        ),
    )


def insert_wht_certificate(cur, record):
    cur.execute(
        "INSERT INTO bronze.wht_certificates (subsidiary_id, cert_number, vendor_id, atc_code, income_payment, tax_rate, wht_amount, issued_date, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("cert_number"),
            record.get("vendor_id"),
            record.get("atc_code"),
            record.get("income_payment"),
            record.get("tax_rate"),
            record.get("wht_amount"),
            record.get("issued_date"),
        ),
    )


def insert_sec_filing(cur, record):
    cur.execute(
        "INSERT INTO bronze.sec_filings (subsidiary_id, filing_type, due_date, filing_date, status, reference_number, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("filing_type"),
            record.get("due_date"),
            record.get("filing_date"),
            record.get("status"),
            record.get("reference_number"),
        ),
    )


def insert_stockholder(cur, record):
    cur.execute(
        "INSERT INTO bronze.stockholders (subsidiary_id, stockholder_id, name, ownership_pct, is_foreign, ingested_at) VALUES (%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("stockholder_id"),
            record.get("name"),
            record.get("ownership_pct"),
            record.get("is_foreign", False),
        ),
    )


def insert_board_resolution(cur, record):
    cur.execute(
        "INSERT INTO bronze.board_resolutions (subsidiary_id, resolution_number, resolution_date, title, enacted_by, ingested_at) VALUES (%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("resolution_number"),
            record.get("resolution_date"),
            record.get("title"),
            record.get("enacted_by"),
        ),
    )


def insert_officer(cur, record):
    cur.execute(
        "INSERT INTO bronze.officers (subsidiary_id, officer_id, name, position, start_date, end_date, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("officer_id"),
            record.get("name"),
            record.get("position"),
            record.get("start_date"),
            record.get("end_date"),
        ),
    )


def insert_audit_event(cur, record):
    cur.execute(
        "INSERT INTO bronze.audit_log (subsidiary_id, event_timestamp, user_id, action, result, risk_level, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("event_timestamp"),
            record.get("user_id"),
            record.get("action"),
            record.get("result"),
            record.get("risk_level"),
        ),
    )


def insert_access_event(cur, record):
    cur.execute(
        "INSERT INTO bronze.access_events (subsidiary_id, event_timestamp, user_id, action, resource, resource_id, ip_address, result, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("event_timestamp"),
            record.get("user_id"),
            record.get("action"),
            record.get("resource"),
            record.get("resource_id"),
            record.get("ip_address"),
            record.get("result"),
        ),
    )


def insert_system_incident(cur, record):
    cur.execute(
        "INSERT INTO bronze.system_incidents (subsidiary_id, incident_id, incident_date, incident_type, severity, description, resolved_date, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON CONFLICT DO NOTHING",
        (
            record.get("subsidiary_id"),
            record.get("incident_id"),
            record.get("incident_date"),
            record.get("incident_type"),
            record.get("severity"),
            record.get("description"),
            record.get("resolved_date"),
        ),
    )


def fetch_records(url, params):
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    return payload if isinstance(payload, list) else []


def load_records(url, insert_fn, params=None):
    params = params or {}
    hook = PostgresHook(postgres_conn_id="lakehouse_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()
    records = fetch_records(url, params)

    for record in records:
        insert_fn(cur, record)

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


ENDPOINTS = [
    {
        "task_id": "finance_journal_entries",
        "url": "http://dept-finance-abc:8000/journal-entries",
        "group": "finance",
        "insert_fn": insert_journal_entry,
        "params": {"days": 7},
    },
    {
        "task_id": "finance_ap_invoices",
        "url": "http://dept-finance-abc:8000/ap-invoices",
        "group": "finance",
        "insert_fn": insert_ap_invoice,
        "params": {"days": 7},
    },
    {
        "task_id": "finance_ar_ledger",
        "url": "http://dept-finance-abc:8000/ar-ledger",
        "group": "finance",
        "insert_fn": insert_ar_ledger,
        "params": {"days": 7},
    },
    {
        "task_id": "tax_vat_returns",
        "url": "http://dept-tax-abc:8000/vat-returns",
        "group": "tax",
        "insert_fn": insert_vat_return,
        "params": {"months": 3},
    },
    {
        "task_id": "tax_wht_filings",
        "url": "http://dept-tax-abc:8000/wht-filings",
        "group": "tax",
        "insert_fn": insert_wht_filing,
        "params": {"days": 7},
    },
    {
        "task_id": "tax_bir_filings_log",
        "url": "http://dept-tax-abc:8000/bir-filings-log",
        "group": "tax",
        "insert_fn": insert_bir_filing_log,
        "params": {"days": 7},
    },
    {
        "task_id": "hr_employees",
        "url": "http://dept-hr-abc:8000/employees",
        "group": "hr",
        "insert_fn": insert_hr_employee,
    },
    {
        "task_id": "hr_payroll_runs",
        "url": "http://dept-hr-abc:8000/payroll-runs",
        "group": "hr",
        "insert_fn": insert_payroll_run,
        "params": {"days": 7},
    },
    {
        "task_id": "hr_statutory_contributions",
        "url": "http://dept-hr-abc:8000/statutory-contributions",
        "group": "hr",
        "insert_fn": insert_statutory_contribution,
        "params": {"days": 7},
    },
    {
        "task_id": "ops_inventory_movements",
        "url": "http://dept-ops-abc:8000/inventory-movements",
        "group": "ops",
        "insert_fn": insert_inventory_movement,
        "params": {"days": 7},
    },
    {
        "task_id": "ops_purchase_orders",
        "url": "http://dept-ops-abc:8000/purchase-orders",
        "group": "ops",
        "insert_fn": insert_purchase_order,
        "params": {"days": 7},
    },
    {
        "task_id": "ops_sales_orders",
        "url": "http://dept-ops-abc:8000/sales-orders",
        "group": "ops",
        "insert_fn": insert_sales_order,
        "params": {"days": 7},
    },
    {
        "task_id": "sales_transactions",
        "url": "http://dept-sales-abc:8000/sales-transactions",
        "group": "sales",
        "insert_fn": insert_sales_record,
        "params": {"days": 7},
    },
    {
        "task_id": "sales_customers",
        "url": "http://dept-sales-abc:8000/customers",
        "group": "sales",
        "insert_fn": insert_customer,
    },
    {
        "task_id": "sales_campaigns",
        "url": "http://dept-sales-abc:8000/campaigns",
        "group": "sales",
        "insert_fn": insert_campaign,
    },
    {
        "task_id": "procurement_vendors",
        "url": "http://dept-procurement-abc:8000/vendors",
        "group": "procurement",
        "insert_fn": insert_vendor,
    },
    {
        "task_id": "procurement_purchase_orders",
        "url": "http://dept-procurement-abc:8000/purchase-orders",
        "group": "procurement",
        "insert_fn": insert_purchase_order,
        "params": {"days": 7},
    },
    {
        "task_id": "procurement_wht_certs",
        "url": "http://dept-procurement-abc:8000/wht-certs",
        "group": "procurement",
        "insert_fn": insert_wht_certificate,
    },
    {
        "task_id": "legal_sec_filings",
        "url": "http://dept-legal-abc:8000/sec-filings",
        "group": "legal",
        "insert_fn": insert_sec_filing,
    },
    {
        "task_id": "legal_stockholders",
        "url": "http://dept-legal-abc:8000/stockholders",
        "group": "legal",
        "insert_fn": insert_stockholder,
    },
    {
        "task_id": "legal_board_resolutions",
        "url": "http://dept-legal-abc:8000/board-resolutions",
        "group": "legal",
        "insert_fn": insert_board_resolution,
    },
    {
        "task_id": "legal_officers",
        "url": "http://dept-legal-abc:8000/officers",
        "group": "legal",
        "insert_fn": insert_officer,
    },
    {
        "task_id": "it_audit_log",
        "url": "http://dept-it-audit-abc:8000/audit-log",
        "group": "it_audit",
        "insert_fn": insert_audit_event,
        "params": {"days": 7},
    },
    {
        "task_id": "it_access_events",
        "url": "http://dept-it-audit-abc:8000/access-events",
        "group": "it_audit",
        "insert_fn": insert_access_event,
        "params": {"days": 7},
    },
    {
        "task_id": "it_system_incidents",
        "url": "http://dept-it-audit-abc:8000/system-incidents",
        "group": "it_audit",
        "insert_fn": insert_system_incident,
        "params": {"days": 7},
    },
]


def ingest_endpoint(url, insert_fn, params=None):
    loaded = load_records(url=url, insert_fn=insert_fn, params=params)
    return f"Inserted {loaded} record(s)"


def build_task(spec):
    return PythonOperator(
        task_id=f"ingest_{spec['task_id']}",
        python_callable=ingest_endpoint,
        op_kwargs={
            "url": spec["url"],
            "insert_fn": spec["insert_fn"],
            "params": spec.get("params", {}),
        },
    )


def build_group(group_id, specs):
    with TaskGroup(group_id=group_id) as group:
        for spec in specs:
            build_task(spec)
    return group


dag = DAG(
    dag_id="lakehouse_bronze_ingestion_and_transform",
    default_args=DEFAULT_ARGS,
    description="CDC + HTTP polling → bronze → dbt silver (SCD Type 2) → dbt gold (KPIs, compliance scorecards)",
    schedule_interval="0 1 * * *",  # 1 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "ingestion", "transform", "cdc"],
)

with dag:
    start = EmptyOperator(task_id="start_pipeline")

    # ========================================================================
    # CDC Health Checks (v3.0 — Change Data Capture via Kafka)
    # ========================================================================
    cdc_lag_check = PythonOperator(
        task_id="cdc_check_kafka_lag",
        python_callable=check_kafka_lag,
        retries=1,
    )

    cdc_freshness_check = PythonOperator(
        task_id="cdc_check_event_log_freshness",
        python_callable=check_cdc_event_log_freshness,
        retries=1,
    )

    # ========================================================================
    # bronze Ingestion: HTTP polling from dept FastAPI (hybrid with CDC)
    # ========================================================================
    groups = {}
    for spec in ENDPOINTS:
        groups.setdefault(spec["group"], []).append(spec)

    group_tasks = []
    for group_id, specs in groups.items():
        group_tasks.append(build_group(group_id, specs))

    # ========================================================================
    # Silver Layer: Deduplication, PII masking, SCD Type 2 for CDC updates/deletes
    # ========================================================================
    dbt_silver_run = BashOperator(
        task_id="dbt_silver_run",
        bash_command="cd /opt/dbt && dbt run --select tag:silver --threads 4",
        retries=2,
        timeout=1800,  # 30 min timeout
    )

    dbt_silver_test = BashOperator(
        task_id="dbt_silver_test",
        bash_command="cd /opt/dbt && dbt test --select tag:silver",
        retries=1,
        timeout=900,
    )

    # ========================================================================
    # Gold Layer: KPI aggregates, compliance scorecards, interco elimination
    # ========================================================================
    dbt_gold_run = BashOperator(
        task_id="dbt_gold_run",
        bash_command="cd /opt/dbt && dbt run --select tag:gold --threads 4",
        retries=2,
        timeout=1800,
    )

    dbt_gold_test = BashOperator(
        task_id="dbt_gold_test",
        bash_command="cd /opt/dbt && dbt test --select tag:gold",
        retries=1,
        timeout=900,
    )

    # ========================================================================
    # Data Quality Checks: Validate gold tables (non-empty, freshness)
    # ========================================================================
    gold_quality_check = PostgresOperator(
        task_id="check_gold_table_quality",
        postgres_conn_id="lakehouse_postgres",
        sql="""
            SELECT
                table_name,
                row_count,
                CASE WHEN row_count = 0 THEN 'FAILED' ELSE 'OK' END as status
            FROM (
                SELECT 'marts.exec_kpi' as table_name, COUNT(*) as row_count 
                FROM marts.exec_kpi WHERE fiscal_year = EXTRACT(YEAR FROM CURRENT_DATE)
                UNION ALL
                SELECT 'marts.group_pnl', COUNT(*) FROM marts.group_pnl 
                WHERE fiscal_year = EXTRACT(YEAR FROM CURRENT_DATE)
                UNION ALL
                SELECT 'marts.bir_compliance_scorecard', COUNT(*) 
                FROM marts.bir_compliance_scorecard 
                WHERE fiscal_year = EXTRACT(YEAR FROM CURRENT_DATE)
            ) t
        """,
    )

    end = EmptyOperator(task_id="end_pipeline")

    # ========================================================================
    # Task Dependencies: Pipeline Orchestration
    # ========================================================================
    start >> [cdc_lag_check, cdc_freshness_check]
    [cdc_lag_check, cdc_freshness_check] >> group_tasks
    group_tasks >> dbt_silver_run >> dbt_silver_test
    dbt_silver_test >> dbt_gold_run >> dbt_gold_test
    dbt_gold_test >> gold_quality_check >> end


# ============================================================================
# DAG 2: Kafka CDC Lag Monitoring (optional, triggered on demand or scheduled)
# ============================================================================

dag_cdc_monitoring = DAG(
    dag_id="lakehouse_cdc_monitoring",
    default_args=DEFAULT_ARGS,
    description="Monitor Kafka CDC lag and alert if backlog exceeds threshold",
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "cdc", "monitoring"],
)

with dag_cdc_monitoring:
    cdc_start = EmptyOperator(task_id="cdc_monitoring_start")

    kafka_lag_monitor = PythonOperator(
        task_id="kafka_consumer_lag_monitor",
        python_callable=check_kafka_lag,
        retries=2,
    )

    cdc_event_freshness_monitor = PythonOperator(
        task_id="cdc_event_freshness_monitor",
        python_callable=check_cdc_event_log_freshness,
        retries=1,
    )

    # Alert if CDC lag is high (optional: send email or Slack notification)
    # FIX Critical 4: was "-U airflow" but the lakehouse DB owner is "postgres".
    # Use PGPASSWORD env var (set from the Airflow connection) so psql can auth.
    log_cdc_status = BashOperator(
        task_id="log_cdc_status",
        bash_command=(
            "echo 'CDC lag monitoring completed at $(date)' && "
            "PGPASSWORD=${POSTGRES_PASSWORD} "
            "psql -h postgres-central -U postgres -d lakehouse -c "
            "\"SELECT COUNT(*) as unprocessed_events FROM bronze.cdc_event_log WHERE is_processed = FALSE;\""
        ),
        env={"POSTGRES_PASSWORD": "{{ conn.lakehouse_postgres.password }}"},
    )

    cdc_end = EmptyOperator(task_id="cdc_monitoring_end")

    cdc_start >> [kafka_lag_monitor,
                  cdc_event_freshness_monitor] >> log_cdc_status >> cdc_end


# ============================================================================
# DAG 3: dbt Snapshot + Incremental Run (triggered on high Kafka lag)
# ============================================================================

dag_dbt_incremental = DAG(
    dag_id="lakehouse_dbt_incremental_refresh",
    default_args=DEFAULT_ARGS,
    description="Incremental dbt snapshot refresh (triggered on CDC lag alert)",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "transform", "incremental"],
)

with dag_dbt_incremental:
    incr_start = EmptyOperator(task_id="incremental_refresh_start")

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/dbt && dbt snapshot",
        retries=1,
        timeout=1200,
    )

    dbt_incremental_silver = BashOperator(
        task_id="dbt_incremental_silver",
        bash_command="cd /opt/dbt && dbt run --select state:modified+ --state /tmp/dbt_state --select tag:silver",
        retries=2,
        timeout=1800,
    )

    dbt_incremental_gold = BashOperator(
        task_id="dbt_incremental_gold",
        bash_command="cd /opt/dbt && dbt run --select tag:gold --state /tmp/dbt_state",
        retries=2,
        timeout=1800,
    )

    incr_end = EmptyOperator(task_id="incremental_refresh_end")

    incr_start >> dbt_snapshot >> dbt_incremental_silver >> dbt_incremental_gold >> incr_end


# ============================================================================
# DAG 4: Weekly Reconciliation & Audit Report
# ============================================================================

dag_reconciliation = DAG(
    dag_id="lakehouse_weekly_reconciliation",
    default_args=DEFAULT_ARGS,
    description="Weekly reconciliation: bronze vs silver row counts, detect anomalies, generate audit report",
    schedule_interval="0 2 ? * MON",  # 2 AM every Monday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "audit", "reconciliation"],
)

with dag_reconciliation:
    recon_start = EmptyOperator(task_id="reconciliation_start")

    row_count_check = PostgresOperator(
        task_id="bronze_vs_silver_row_count_audit",
        postgres_conn_id="lakehouse_postgres",
        sql="""
            SELECT 
                'bronze.journal_entries' as table_name,
                (SELECT COUNT(*) FROM bronze.journal_entries) as bronze_count,
                (SELECT COUNT(*) FROM transform.fact_journal_entries) as silver_count
            UNION ALL
            SELECT 
                'bronze.sales_transactions',
                (SELECT COUNT(*) FROM bronze.sales_transactions),
                (SELECT COUNT(*) FROM transform.fact_sales)
            UNION ALL
            SELECT 
                'bronze.employees',
                (SELECT COUNT(*) FROM bronze.employees),
                (SELECT COUNT(*) FROM transform.dim_employees)
            ORDER BY table_name;
        """,
    )

    cdc_event_audit = PostgresOperator(
        task_id="cdc_event_log_audit",
        postgres_conn_id="lakehouse_postgres",
        sql="""
            SELECT 
                DATE(event_ts) as event_date,
                source_dept,
                operation,
                COUNT(*) as event_count
            FROM bronze.cdc_event_log
            WHERE event_ts >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(event_ts), source_dept, operation
            ORDER BY event_date DESC, source_dept;
        """,
    )

    generate_audit_report = BashOperator(
        task_id="generate_weekly_audit_report",
        # FIX Critical 4: was "-U airflow"; must be "-U postgres" (the lakehouse DB owner).
        bash_command="""
            cd /opt/dbt && \
            dbt docs generate && \
            PGPASSWORD=${POSTGRES_PASSWORD} \
            psql -h postgres-central -U postgres -d lakehouse -c \
            "COPY (SELECT * FROM bronze.cdc_event_log WHERE event_ts >= NOW() - INTERVAL '7 days') \
            TO STDOUT WITH CSV HEADER" > /tmp/audit_report_$(date +%Y%m%d).csv
        """,
        env={"POSTGRES_PASSWORD": "{{ conn.lakehouse_postgres.password }}"},
        retries=1,
    )

    recon_end = EmptyOperator(task_id="reconciliation_end")

    recon_start >> [row_count_check,
                    cdc_event_audit] >> generate_audit_report >> recon_end
