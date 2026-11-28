"""B5: weather_history DAG (manual).

fetch_weather_history -> load_weather(is_forecast=False).
"""

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _fetch_weather_history() -> str:
    from bikeflow.ingest.weather import fetch_weather_history

    return str(fetch_weather_history())


def _load_weather(path: str) -> int:
    from bikeflow.load.loaders import load_weather

    return load_weather(path, is_forecast=False)


with DAG(
    dag_id="weather_history",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bikeflow"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_weather_history", python_callable=_fetch_weather_history
    )
    load = PythonOperator(
        task_id="load_weather",
        python_callable=_load_weather,
        op_kwargs={"path": "{{ ti.xcom_pull(task_ids='fetch_weather_history') }}"},
    )

    fetch >> load
