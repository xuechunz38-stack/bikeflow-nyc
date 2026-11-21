"""A6: sanity checks so nobody breaks the shared fixtures by accident."""

import json
from pathlib import Path

import polars as pl

from bikeflow.ingest.stations import stations_from_gbfs

FIX = Path(__file__).parent / "fixtures"


def test_trips_sample_has_real_rows_and_manual_cases():
    df = pl.read_csv(FIX / "trips_sample.csv", infer_schema=False)
    assert df.height == 2010
    codes = df.filter(pl.col("ride_id").str.starts_with("FIXTURE_CODE"))["start_station_id"]
    assert codes.to_list() == [
        "5980.10", "5980.1", "5303.06_", "5348.06_old", "5980.123", "SYS016", "Shop Morgan", None
    ]
    assert df.filter(pl.col("ride_id").str.starts_with("FIXTURE_DUR")).height == 2


def test_stations_sample_parses_and_covers_cases():
    df = stations_from_gbfs(json.loads((FIX / "stations_sample.json").read_text()))
    assert {"5980.10", "5348.06", "5303.06", "6560.01"} <= set(df["station_code"])


def test_weather_sample_is_full_history():
    hourly = json.loads((FIX / "weather_sample.json").read_text())["hourly"]
    assert len(hourly["time"]) == 2208 == len(set(hourly["time"]))
    assert hourly["time"][0] == "2026-07-01T00:00"
    assert hourly["time"][-1] == "2026-09-30T23:00"
