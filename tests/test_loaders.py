"""B3: loader tests (needs the Postgres from compose or CI).

Uses made-up keys (station 0.01/0.02, year 2000 weather) and deletes them
before and after, so real data in the dev database is not touched.
"""

from datetime import datetime

import polars as pl
import pytest

from bikeflow.db import get_conn
from bikeflow.load.loaders import load_station_hour, load_stations, load_weather

CODES = ["0.01", "0.02"]
HOURS = [datetime.fromisoformat(f"2000-01-01T0{h}:00") for h in range(3)]
CUTOFF = "2001-01-01"


def _cleanup():
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM fact_station_hour WHERE station_code = ANY(%s)", [CODES]
        )
        conn.execute("DELETE FROM dim_station WHERE station_code = ANY(%s)", [CODES])
        conn.execute(
            "DELETE FROM weather_hour WHERE hour_start < %s::timestamp", [CUTOFF]
        )


def _count(sql: str, params: list) -> int:
    with get_conn() as conn:
        return conn.execute(sql, params).fetchone()[0]


@pytest.fixture(autouse=True)
def clean_db():
    _cleanup()
    yield
    _cleanup()


@pytest.fixture
def stations_file(tmp_path):
    df = pl.DataFrame(
        {
            "station_code": CODES,
            "gbfs_station_id": ["test-a", "test-b"],
            "name": ["Test A", "Test B"],
            "lat": [40.7, 40.8],
            "lon": [-74.0, -73.9],
            "capacity": [20, 30],
        },
        schema={
            "station_code": pl.Utf8,
            "gbfs_station_id": pl.Utf8,
            "name": pl.Utf8,
            "lat": pl.Float64,
            "lon": pl.Float64,
            "capacity": pl.Int32,
        },
    )
    path = tmp_path / "stations.parquet"
    df.write_parquet(path)
    return path


def test_load_stations_twice_keeps_count(stations_file):
    assert load_stations(stations_file) == 2
    assert load_stations(stations_file) == 2
    n = _count("SELECT count(*) FROM dim_station WHERE station_code = ANY(%s)", [CODES])
    assert n == 2


def test_load_stations_updates_values(stations_file, tmp_path):
    load_stations(stations_file)
    changed = pl.read_parquet(stations_file).with_columns(
        pl.lit(99, dtype=pl.Int32).alias("capacity")
    )
    path = tmp_path / "changed.parquet"
    changed.write_parquet(path)
    load_stations(path)
    cap = _count("SELECT capacity FROM dim_station WHERE station_code = %s", ["0.01"])
    assert cap == 99


def test_load_station_hour_twice_keeps_count(stations_file, tmp_path):
    load_stations(stations_file)
    grid = pl.DataFrame(
        {
            "station_code": [c for c in CODES for _ in HOURS],
            "hour_start": HOURS * len(CODES),
            "pickups": [1, 0, 2, 0, 3, 0],
            "dropoffs": [0, 1, 0, 2, 0, 3],
        },
        schema={
            "station_code": pl.Utf8,
            "hour_start": pl.Datetime("us"),
            "pickups": pl.Int32,
            "dropoffs": pl.Int32,
        },
    )
    path = tmp_path / "station_hour_200001.parquet"
    grid.write_parquet(path)
    assert load_station_hour(path) == 6
    assert load_station_hour(path) == 6
    sql = "SELECT count(*), sum(pickups) FROM fact_station_hour WHERE station_code = ANY(%s)"
    with get_conn() as conn:
        n, total = conn.execute(sql, [CODES]).fetchone()
    assert (n, total) == (6, 6)


def test_load_weather_sets_is_forecast(tmp_path):
    df = pl.DataFrame(
        {
            "hour_start": HOURS,
            "temperature_c": [1.0, 2.0, None],
            "precipitation_mm": [0.0, 0.5, 0.0],
            "wind_speed_kmh": [10.0, 12.0, 8.0],
        },
        schema={
            "hour_start": pl.Datetime("us"),
            "temperature_c": pl.Float64,
            "precipitation_mm": pl.Float64,
            "wind_speed_kmh": pl.Float64,
        },
    )
    path = tmp_path / "weather.parquet"
    df.write_parquet(path)
    assert load_weather(path, is_forecast=False) == 3
    assert load_weather(path, is_forecast=False) == 3
    sql = (
        "SELECT count(*), bool_or(is_forecast), count(temperature_c) "
        "FROM weather_hour WHERE hour_start < %s::timestamp"
    )
    with get_conn() as conn:
        n, any_forecast, n_temp = conn.execute(sql, [CUTOFF]).fetchone()
    assert (n, any_forecast, n_temp) == (3, False, 2)
