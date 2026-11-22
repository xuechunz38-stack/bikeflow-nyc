"""A6: hourly weather from Open-Meteo (files F3 and F4)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import requests

HISTORY_URL = (
    "https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.0060"
    "&start_date=2026-07-01&end_date=2026-09-30"
    "&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=America/New_York"
)
FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast?latitude=40.7128&longitude=-74.0060"
    "&hourly=temperature_2m,precipitation,wind_speed_10m&forecast_hours=48"
    "&timezone=America/New_York"
)
WEATHER_DIR = Path("data/processed/weather")
HISTORY_PATH = WEATHER_DIR / "weather_history.parquet"
FORECAST_PATH = WEATHER_DIR / "weather_forecast.parquet"
HISTORY_ROWS = 2_208  # 2026-07-01 00:00 .. 2026-09-30 23:00
FORECAST_ROWS = 48

_UNITS = {"temperature_2m": "°C", "precipitation": "mm", "wind_speed_10m": "km/h"}


def weather_from_open_meteo(payload: dict, expected_rows: int) -> pl.DataFrame:
    """A6: turn an Open-Meteo hourly response into the F3/F4 table.

    Input: parsed JSON (requested with timezone=America/New_York) and the
    row count the file must have.
    Output: hour_start (Datetime us, New York local, no tz), temperature_c,
    precipitation_mm, wind_speed_kmh (Float64), sorted by hour_start.
    Raises ValueError on wrong units, wrong row count or duplicate hours.
    """
    units = payload.get("hourly_units", {})
    for field, unit in _UNITS.items():
        if units.get(field, unit) != unit:
            raise ValueError(f"{field} unit is {units[field]!r}, expected {unit!r}")
    h = payload["hourly"]
    df = (
        pl.DataFrame(
            {
                "hour_start": h["time"],
                "temperature_c": h["temperature_2m"],
                "precipitation_mm": h["precipitation"],
                "wind_speed_kmh": h["wind_speed_10m"],
            },
            schema={
                "hour_start": pl.Utf8,
                "temperature_c": pl.Float64,
                "precipitation_mm": pl.Float64,
                "wind_speed_kmh": pl.Float64,
            },
        )
        .with_columns(
            pl.col("hour_start").str.to_datetime("%Y-%m-%dT%H:%M", time_unit="us")
        )
        .sort("hour_start")
    )
    if df.height != expected_rows:
        raise ValueError(f"expected {expected_rows} hourly rows, got {df.height}")
    if not df["hour_start"].is_unique().all():
        raise ValueError("duplicate hour_start in weather data")
    return df


def _fetch(url: str, expected_rows: int, path: Path) -> Path:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    df = weather_from_open_meteo(resp.json(), expected_rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def fetch_weather_history() -> Path:
    """A6: write F3, hourly weather for 2026-07-01 00:00 to 2026-09-30 23:00.

    Output: path to data/processed/weather/weather_history.parquet (2,208 rows).
    """
    return _fetch(HISTORY_URL, HISTORY_ROWS, HISTORY_PATH)


def fetch_weather_forecast() -> Path:
    """A6: write F4, the next 48 hours of forecast from the current hour.

    Output: path to data/processed/weather/weather_forecast.parquet (48 rows),
    fully overwritten on every run.
    """
    return _fetch(FORECAST_URL, FORECAST_ROWS, FORECAST_PATH)
