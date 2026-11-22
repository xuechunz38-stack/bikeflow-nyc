"""B5: DAG structure tests. Skipped where Airflow is not installed (local venv, CI)."""

from itertools import pairwise

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag

EXPECTED = {
    "trips_monthly": [
        "download_month",
        "fetch_stations",
        "load_stations",
        "build_station_hour",
        "run_checks",
        "load_station_hour",
    ],
    "weather_history": ["fetch_weather_history", "load_weather"],
    "gbfs_status": ["fetch_and_load_status"],
    "forecast_hourly": ["fetch_weather_forecast", "load_weather", "predict_next_hours"],
}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


@pytest.mark.parametrize("dag_id,chain", EXPECTED.items())
def test_task_chain(dagbag, dag_id, chain):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert [t.task_id for t in dag.topological_sort()] == chain
    for up, down in pairwise(chain):
        assert down in dag.get_task(up).downstream_task_ids
