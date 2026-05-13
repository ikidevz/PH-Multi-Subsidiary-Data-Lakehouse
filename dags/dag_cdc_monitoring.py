"""
DAG 2: lakehouse_cdc_monitoring
Schedule: Every 30 minutes
Purpose: Monitor Kafka CDC consumer lag and alert if backlog exceeds threshold.
"""

from shared.cdc_checks import check_kafka_lag, check_cdc_event_log_freshness
from shared.config import DEFAULT_ARGS
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


dag = DAG(
    dag_id="lakehouse_cdc_monitoring",
    default_args=DEFAULT_ARGS,
    description="Monitor Kafka CDC lag and alert if backlog exceeds threshold",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "cdc", "monitoring"],
)

with dag:
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

    # Log CDC status and check unprocessed events count.
    # Uses PGPASSWORD env var (set from the Airflow connection) so psql can auth.
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

    # Task Dependencies
    cdc_start >> [kafka_lag_monitor,
                  cdc_event_freshness_monitor] >> log_cdc_status >> cdc_end
