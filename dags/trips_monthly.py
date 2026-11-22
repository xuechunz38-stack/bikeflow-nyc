"""B5: trips_monthly DAG (manual, param month).

download_month -> fetch_stations -> load_stations -> build_station_hour
-> run_checks -> load_station_hour. Only calls functions from bikeflow/;
imports happen inside the tasks so the DAG still parses if a module is missing.
"""

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


def _download_month(month: str) -> str:
    from bikeflow.ingest.trips import download_month

    return str(download_month(month))


def _fetch_stations() -> str:
    from bikeflow.ingest.stations import fetch_stations

    return str(fetch_stations())


def _load_stations(path: str) -> int:
    from bikeflow.load.loaders import load_stations

    return load_stations(path)


def _build_station_hour(month: str) -> str:
    from bikeflow.transform.aggregate import build_station_hour

    return str(build_station_hour(month))


def _run_checks(month: str) -> dict:
    from bikeflow.quality.checks import run_checks

    return run_checks(month)


def _load_station_hour(path: str) -> int:
    from bikeflow.load.loaders import load_station_hour

    return load_station_hour(path)


with DAG(
    dag_id="trips_monthly",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"month": Param("2026-09", type="string", pattern=r"^\d{4}-\d{2}$")},
    tags=["bikeflow"],
) as dag:
    month = "{{ params.month }}"
    download = PythonOperator(
        task_id="download_month",
        python_callable=_download_month,
        op_kwargs={"month": month},
    )
    stations = PythonOperator(task_id="fetch_stations", python_callable=_fetch_stations)
    load_st = PythonOperator(
        task_id="load_stations",
        python_callable=_load_stations,
        op_kwargs={"path": "{{ ti.xcom_pull(task_ids='fetch_stations') }}"},
    )
    build = PythonOperator(
        task_id="build_station_hour",
        python_callable=_build_station_hour,
        op_kwargs={"month": month},
    )
    checks = PythonOperator(
        task_id="run_checks", python_callable=_run_checks, op_kwargs={"month": month}
    )
    load_sh = PythonOperator(
        task_id="load_station_hour",
        python_callable=_load_station_hour,
        op_kwargs={"path": "{{ ti.xcom_pull(task_ids='build_station_hour') }}"},
    )

    download >> stations >> load_st >> build >> checks >> load_sh
