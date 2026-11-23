"""A8: integration test, fixtures -> A's files -> B's loaders -> Postgres.

Runs clean_trips -> build_station_hour -> fetch_weather_* (network replaced
by tests/fixtures) -> load_stations / load_station_hour / load_weather, then
checks that every table has exactly the rows of the file it was loaded from.

Needs the Postgres from compose or CI (POSTGRES_* env vars). Everything is
written into a throwaway schema (PGOPTIONS search_path), which is dropped at
the end, so real data in the dev database is never touched. No network.
"""

import copy
import json
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from bikeflow.db import get_conn
from bikeflow.ingest import weather
from bikeflow.load.loaders import load_station_hour, load_stations, load_weather
from bikeflow.transform.aggregate import build_station_hour
from bikeflow.transform.clean import clean_trips

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "sql" / "schema.sql"
WEATHER_SAMPLE = json.loads(
    (ROOT / "tests" / "fixtures" / "weather_sample.json").read_text()
)
TEST_SCHEMA = "a8_integration"


def _forecast_payload() -> dict:
    """48 forecast hours from the weather fixture, moved to 2026-10-01 so they
    do not share hour_start keys with the July-September history."""
    p = copy.deepcopy(WEATHER_SAMPLE)
    p["hourly"] = {k: v[:48] for k, v in p["hourly"].items()}
    start = datetime(2026, 10, 1)
    p["hourly"]["time"] = [
        (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(48)
    ]
    return p


class _Resp:
    def __init__(self, payload: dict):
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self.payload


def _fake_get(url: str, timeout: int) -> _Resp:
    """Stand-in for requests.get: the archive URL gets the history fixture,
    the forecast URL gets 48 hours of it."""
    if url == weather.HISTORY_URL:
        return _Resp(WEATHER_SAMPLE)
    if url == weather.FORECAST_URL:
        return _Resp(_forecast_payload())
    raise AssertionError(f"unexpected URL {url}")


def _one(sql: str):
    with get_conn() as conn:
        return conn.execute(sql).fetchone()


@pytest.fixture
def test_schema(monkeypatch):
    """Create a fresh schema with sql/schema.sql and point every new
    connection at it; drop it afterwards."""
    with get_conn() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
        conn.execute(f"CREATE SCHEMA {TEST_SCHEMA}")
        conn.execute(f"SET search_path TO {TEST_SCHEMA}")
        conn.execute(SCHEMA_SQL.read_text())
    monkeypatch.setenv("PGOPTIONS", f"-c search_path={TEST_SCHEMA}")
    yield
    with get_conn() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")


def test_files_load_into_tables_with_matching_counts(workdir, test_schema, monkeypatch):
    # A3 + A4: trips fixture (as 2026-09 raw data) -> F2.
    trips = clean_trips("2026-09")
    f2_path = build_station_hour("2026-09")
    f1 = pl.read_parquet("data/processed/stations.parquet")
    f2 = pl.read_parquet(f2_path)
    assert f2["pickups"].sum() == trips["start_code"].is_not_null().sum()

    # A6: F3 and F4 through the real fetch functions, network replaced.
    monkeypatch.setattr(weather.requests, "get", _fake_get)
    f3 = pl.read_parquet(weather.fetch_weather_history())
    f4 = pl.read_parquet(weather.fetch_weather_forecast())
    assert (f3.height, f4.height) == (2208, 48)

    # B3: load every file. dim_station first (foreign key of fact_station_hour).
    assert load_stations("data/processed/stations.parquet") == f1.height
    assert load_station_hour(f2_path) == f2.height
    assert load_weather(weather.HISTORY_PATH, is_forecast=False) == f3.height
    assert load_weather(weather.FORECAST_PATH, is_forecast=True) == f4.height

    # Every table holds exactly the rows of its file.
    assert _one("SELECT count(*) FROM dim_station")[0] == f1.height
    assert _one(
        "SELECT count(*), sum(pickups), sum(dropoffs) FROM fact_station_hour"
    ) == (f2.height, f2["pickups"].sum(), f2["dropoffs"].sum())
    assert _one(
        "SELECT count(*) FILTER (WHERE NOT is_forecast), "
        "count(*) FILTER (WHERE is_forecast) FROM weather_hour"
    ) == (f3.height, f4.height)

    # Nothing leaked into the real tables' schema: the loaders wrote here.
    assert _one("SELECT current_schema()")[0] == TEST_SCHEMA
