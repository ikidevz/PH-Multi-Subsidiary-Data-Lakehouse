"""
DAG 1: lakehouse_bronze_ingestion_and_transform
Schedule: Daily at 1 AM
Purpose: CDC health checks → HTTP polling from dept FastAPI endpoints
         → bronze layer → dbt silver (SCD Type 2) → dbt gold (KPIs, compliance)
"""

from inserts.it_audit_inserts import insert_audit_event, insert_access_event, insert_system_incident
from inserts.legal_inserts import insert_sec_filing, insert_stockholder, insert_board_resolution, insert_officer
from inserts.procurement_inserts import (
    insert_vendor,
    insert_purchase_order as procurement_insert_purchase_order,
    insert_wht_certificate,
)
from inserts.sales_inserts import insert_sales_record, insert_customer, insert_campaign
from inserts.ops_inserts import (
    insert_inventory_movement,
    insert_purchase_order as ops_insert_purchase_order,
    insert_sales_order,
)
from inserts.hr_inserts import insert_hr_employee, insert_payroll_run, insert_statutory_contribution
from inserts.tax_inserts import insert_vat_return, insert_wht_filing, insert_bir_filing_log
from inserts.finance_inserts import insert_journal_entry, insert_ap_invoice, insert_ar_ledger
from shared.loader import ingest_endpoint
from shared.cdc_checks import check_kafka_lag, check_cdc_event_log_freshness
from shared.config import DEFAULT_ARGS
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Domain insert functions

# ============================================================================
# Endpoint Registry
# ============================================================================
ENDPOINTS = [
    # Finance
    {
        "task_id": "finance_journal_entries",
        "url": "http://dept-finance-abc:8001/journal-entries",
        "group": "finance",
        "insert_fn": insert_journal_entry,
        "params": {"days": 7},
    },
    {
        "task_id": "finance_ap_invoices",
        "url": "http://dept-finance-abc:8001/ap-invoices",
        "group": "finance",
        "insert_fn": insert_ap_invoice,
        "params": {"days": 7},
    },
    {
        "task_id": "finance_ar_ledger",
        "url": "http://dept-finance-abc:8001/ar-ledger",
        "group": "finance",
        "insert_fn": insert_ar_ledger,
        "params": {"days": 7},
    },
    # Tax
    {
        "task_id": "tax_vat_returns",
        "url": "http://dept-tax-abc:8002/vat-returns",
        "group": "tax",
        "insert_fn": insert_vat_return,
        "params": {"months": 3},
    },
    {
        "task_id": "tax_wht_filings",
        "url": "http://dept-tax-abc:8002/wht-filings",
        "group": "tax",
        "insert_fn": insert_wht_filing,
        "params": {"days": 7},
    },
    {
        "task_id": "tax_bir_filings_log",
        "url": "http://dept-tax-abc:8002/bir-filings-log",
        "group": "tax",
        "insert_fn": insert_bir_filing_log,
        "params": {"days": 7},
    },
    # HR
    {
        "task_id": "hr_employees",
        "url": "http://dept-hr-abc:8003/employees",
        "group": "hr",
        "insert_fn": insert_hr_employee,
    },
    {
        "task_id": "hr_payroll_runs",
        "url": "http://dept-hr-abc:8003/payroll-runs",
        "group": "hr",
        "insert_fn": insert_payroll_run,
        "params": {"days": 7},
    },
    {
        "task_id": "hr_statutory_contributions",
        "url": "http://dept-hr-abc:8003/statutory-contributions",
        "group": "hr",
        "insert_fn": insert_statutory_contribution,
        "params": {"days": 7},
    },
    # Ops
    {
        "task_id": "ops_inventory_movements",
        "url": "http://dept-ops-abc:8004/inventory-movements",
        "group": "ops",
        "insert_fn": insert_inventory_movement,
        "params": {"days": 7},
    },
    {
        "task_id": "ops_purchase_orders",
        "url": "http://dept-ops-abc:8004/purchase-orders",
        "group": "ops",
        "insert_fn": ops_insert_purchase_order,
        "params": {"days": 7},
    },
    {
        "task_id": "ops_sales_orders",
        "url": "http://dept-ops-abc:8004/sales-orders",
        "group": "ops",
        "insert_fn": insert_sales_order,
        "params": {"days": 7},
    },
    # Sales
    {
        "task_id": "sales_transactions",
        "url": "http://dept-sales-abc:8005/sales-transactions",
        "group": "sales",
        "insert_fn": insert_sales_record,
        "params": {"days": 7},
    },
    {
        "task_id": "sales_customers",
        "url": "http://dept-sales-abc:8005/customers",
        "group": "sales",
        "insert_fn": insert_customer,
    },
    {
        "task_id": "sales_campaigns",
        "url": "http://dept-sales-abc:8005/campaigns",
        "group": "sales",
        "insert_fn": insert_campaign,
    },
    # Procurement
    {
        "task_id": "procurement_vendors",
        "url": "http://dept-procurement-abc:8006/vendors",
        "group": "procurement",
        "insert_fn": insert_vendor,
    },
    {
        "task_id": "procurement_purchase_orders",
        "url": "http://dept-procurement-abc:8006/purchase-orders",
        "group": "procurement",
        "insert_fn": procurement_insert_purchase_order,
        "params": {"days": 7},
    },
    {
        "task_id": "procurement_wht_certs",
        "url": "http://dept-procurement-abc:8006/wht-certs",
        "group": "procurement",
        "insert_fn": insert_wht_certificate,
    },
    # Legal
    {
        "task_id": "legal_sec_filings",
        "url": "http://dept-legal-abc:8007/sec-filings",
        "group": "legal",
        "insert_fn": insert_sec_filing,
    },
    {
        "task_id": "legal_stockholders",
        "url": "http://dept-legal-abc:8007/stockholders",
        "group": "legal",
        "insert_fn": insert_stockholder,
    },
    {
        "task_id": "legal_board_resolutions",
        "url": "http://dept-legal-abc:8007/board-resolutions",
        "group": "legal",
        "insert_fn": insert_board_resolution,
    },
    {
        "task_id": "legal_officers",
        "url": "http://dept-legal-abc:8007/officers",
        "group": "legal",
        "insert_fn": insert_officer,
    },
    # IT Audit
    {
        "task_id": "it_audit_log",
        "url": "http://dept-it-audit-abc:8008/audit-log",
        "group": "it_audit",
        "insert_fn": insert_audit_event,
        "params": {"days": 7},
    },
    {
        "task_id": "it_access_events",
        "url": "http://dept-it-audit-abc:8008/access-events",
        "group": "it_audit",
        "insert_fn": insert_access_event,
        "params": {"days": 7},
    },
    {
        "task_id": "it_system_incidents",
        "url": "http://dept-it-audit-abc:8008/system-incidents",
        "group": "it_audit",
        "insert_fn": insert_system_incident,
        "params": {"days": 7},
    },
]


# ============================================================================
# Task Builders
# ============================================================================
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


# ============================================================================
# DAG Definition
# ============================================================================
dag = DAG(
    dag_id="lakehouse_bronze_ingestion_and_transform",
    default_args=DEFAULT_ARGS,
    description=(
        "CDC + HTTP polling → bronze → dbt silver (SCD Type 2) → dbt gold (KPIs, compliance scorecards)"
    ),
    schedule="0 1 * * *",  # 1 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "ingestion", "transform", "cdc"],
)

with dag:
    start = EmptyOperator(task_id="start_pipeline")

    # ------------------------------------------------------------------
    # CDC Health Checks
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # bronze Ingestion: HTTP polling (hybrid with CDC), grouped by domain
    # ------------------------------------------------------------------
    groups = {}
    for spec in ENDPOINTS:
        groups.setdefault(spec["group"], []).append(spec)

    group_tasks = [build_group(group_id, specs)
                   for group_id, specs in groups.items()]

    # ------------------------------------------------------------------
    # Silver Layer: Deduplication, PII masking, SCD Type 2
    # ------------------------------------------------------------------
    dbt_silver_run = BashOperator(
        task_id="dbt_silver_run",
        bash_command="cd /opt/dbt && dbt run --select tag:silver --threads 4",
        retries=2,
        timeout=1800,
    )

    dbt_silver_test = BashOperator(
        task_id="dbt_silver_test",
        bash_command="cd /opt/dbt && dbt test --select tag:silver",
        retries=1,
        timeout=900,
    )

    # ------------------------------------------------------------------
    # Gold Layer: KPI aggregates, compliance scorecards, interco elimination
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Data Quality Checks: Validate gold tables (non-empty, freshness)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Task Dependencies
    # ------------------------------------------------------------------
    start >> [cdc_lag_check, cdc_freshness_check]
    [cdc_lag_check, cdc_freshness_check] >> group_tasks
    group_tasks >> dbt_silver_run >> dbt_silver_test
    dbt_silver_test >> dbt_gold_run >> dbt_gold_test
    dbt_gold_test >> gold_quality_check >> end
