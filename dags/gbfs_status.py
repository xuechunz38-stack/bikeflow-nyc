"""B5: gbfs_status DAG (every 15 minutes).

fetch_and_load_status.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _fetch_and_load_status() -> int:
    from bikeflow.ingest.gbfs_status import fetch_and_load_status

    return fetch_and_load_status()


with DAG(
    dag_id="gbfs_status",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["bikeflow"],
) as dag:
    PythonOperator(
        task_id="fetch_and_load_status", python_callable=_fetch_and_load_status
    )
