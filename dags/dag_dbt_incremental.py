"""
DAG 3: lakehouse_dbt_incremental_refresh
Schedule: Manual trigger only (triggered on high Kafka CDC lag)
Purpose: Incremental dbt snapshot + silver/gold refresh to catch up on CDC backlog.
"""

from shared.config import DEFAULT_ARGS
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


dag = DAG(
    dag_id="lakehouse_dbt_incremental_refresh",
    default_args=DEFAULT_ARGS,
    description="Incremental dbt snapshot refresh (triggered on CDC lag alert)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "transform", "incremental"],
)

with dag:
    incr_start = EmptyOperator(task_id="incremental_refresh_start")

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/dbt && dbt snapshot",
        retries=1,
        timeout=1200,
    )

    dbt_incremental_silver = BashOperator(
        task_id="dbt_incremental_silver",
        bash_command=(
            "cd /opt/dbt && "
            "dbt compile --target prod && "
            "dbt run --select tag:silver --threads 4"
        ),
        retries=2,
        timeout=1800,
    )

    dbt_incremental_gold = BashOperator(
        task_id="dbt_incremental_gold",
        bash_command="cd /opt/dbt && dbt run --select tag:gold --threads 4",
        retries=2,
        timeout=1800,
    )

    incr_end = EmptyOperator(task_id="incremental_refresh_end")

    # Task Dependencies
    incr_start >> dbt_snapshot >> dbt_incremental_silver >> dbt_incremental_gold >> incr_end
