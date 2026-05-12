"""
DAG 3: lakehouse_dbt_incremental_refresh
Schedule: Manual trigger only (triggered on high Kafka CDC lag)
Purpose: Incremental dbt snapshot + silver/gold refresh to catch up on CDC backlog.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from shared.config import DEFAULT_ARGS

dag = DAG(
    dag_id="lakehouse_dbt_incremental_refresh",
    default_args=DEFAULT_ARGS,
    description="Incremental dbt snapshot refresh (triggered on CDC lag alert)",
    schedule_interval=None,  # Manual trigger only
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
            "cd /opt/dbt && dbt run "
            "--select state:modified+ --state /tmp/dbt_state "
            "--select tag:silver"
        ),
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

    # Task Dependencies
    incr_start >> dbt_snapshot >> dbt_incremental_silver >> dbt_incremental_gold >> incr_end
