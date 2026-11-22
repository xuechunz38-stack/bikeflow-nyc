"""Tests for A6 weather, using tests/fixtures/weather_sample.json (no network)."""

import copy
import json
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from bikeflow.ingest import weather

SAMPLE = json.loads((Path(__file__).parent / "fixtures" / "weather_sample.json").read_text())
F3_SCHEMA = pl.Schema(
    {
        "hour_start": pl.Datetime("us"),
        "temperature_c": pl.Float64,
        "precipitation_mm": pl.Float64,
        "wind_speed_kmh": pl.Float64,
    }
)


def _forecast_payload():
    p = copy.deepcopy(SAMPLE)
    p["hourly"] = {k: v[:48] for k, v in p["hourly"].items()}
    return p


class Resp:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


def test_history_parses_to_f3():
    df = weather.weather_from_open_meteo(SAMPLE, weather.HISTORY_ROWS)
    assert df.schema == F3_SCHEMA
    assert df.height == 2208
    assert df["hour_start"].is_unique().all()
    assert df["hour_start"][0] == datetime(2026, 7, 1, 0)
    assert df["hour_start"][-1] == datetime(2026, 9, 30, 23)


def test_wrong_row_count_raises():
    with pytest.raises(ValueError, match="2208"):
        weather.weather_from_open_meteo(_forecast_payload(), weather.HISTORY_ROWS)


def test_duplicate_hour_raises():
    p = _forecast_payload()
    p["hourly"]["time"][1] = p["hourly"]["time"][0]
    with pytest.raises(ValueError, match="duplicate"):
        weather.weather_from_open_meteo(p, 48)


def test_wrong_unit_raises():
    p = _forecast_payload()
    p["hourly_units"]["wind_speed_10m"] = "m/s"
    with pytest.raises(ValueError, match="wind_speed_10m"):
        weather.weather_from_open_meteo(p, 48)


def test_fetchers_write_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    urls = {weather.HISTORY_URL: SAMPLE, weather.FORECAST_URL: _forecast_payload()}
    monkeypatch.setattr(weather.requests, "get", lambda url, **kw: Resp(urls[url]))

    p3 = weather.fetch_weather_history()
    assert p3.as_posix() == "data/processed/weather/weather_history.parquet"
    assert pl.read_parquet(p3).height == 2208

    p4 = weather.fetch_weather_forecast()
    assert p4.as_posix() == "data/processed/weather/weather_forecast.parquet"
    assert pl.read_parquet(p4).schema == F3_SCHEMA
    assert pl.read_parquet(p4).height == 48
    weather.fetch_weather_forecast()  # second run overwrites, not appends
    assert pl.read_parquet(p4).height == 48
