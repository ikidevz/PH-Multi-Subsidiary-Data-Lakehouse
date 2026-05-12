"""
DAG 4: lakehouse_weekly_reconciliation
Schedule: 2 AM every Monday
Purpose: bronze vs silver row count audit, CDC event log audit, weekly report export.
"""

from shared.config import DEFAULT_ARGS
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


dag = DAG(
    dag_id="lakehouse_weekly_reconciliation",
    default_args=DEFAULT_ARGS,
    description="Weekly reconciliation: bronze vs silver row counts, detect anomalies, generate audit report",
    schedule_interval="0 2 ? * MON",  # 2 AM every Monday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "audit", "reconciliation"],
)

with dag:
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

    # Generates dbt docs and exports the last 7 days of CDC events as CSV.
    # Uses -U postgres (the lakehouse DB owner) with PGPASSWORD env var for auth.
    generate_audit_report = BashOperator(
        task_id="generate_weekly_audit_report",
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

    # Task Dependencies
    recon_start >> [row_count_check,
                    cdc_event_audit] >> generate_audit_report >> recon_end
