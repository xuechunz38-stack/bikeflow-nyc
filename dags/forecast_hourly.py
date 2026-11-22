"""B5: forecast_hourly DAG (every hour at minute 5).

fetch_weather_forecast -> load_weather(is_forecast=True) -> predict_next_hours (C3).
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _fetch_weather_forecast() -> str:
    from bikeflow.ingest.weather import fetch_weather_forecast

    return str(fetch_weather_forecast())


def _load_weather(path: str) -> int:
    from bikeflow.load.loaders import load_weather

    return load_weather(path, is_forecast=True)


def _predict_next_hours() -> int:
    from bikeflow.model.predict import predict_next_hours

    return predict_next_hours()


with DAG(
    dag_id="forecast_hourly",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bikeflow"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_weather_forecast", python_callable=_fetch_weather_forecast
    )
    load = PythonOperator(
        task_id="load_weather",
        python_callable=_load_weather,
        op_kwargs={"path": "{{ ti.xcom_pull(task_ids='fetch_weather_forecast') }}"},
    )
    predict = PythonOperator(
        task_id="predict_next_hours", python_callable=_predict_next_hours
    )

    fetch >> load >> predict
