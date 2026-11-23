"""C3: live forecasts for the next hours (spec section 7, C3)."""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import polars as pl

from bikeflow.db import get_conn
from bikeflow.model.features import FEATURE_COLUMNS, build_features
from bikeflow.model.train import write_forecasts

ARTIFACTS = Path("artifacts")
NY = ZoneInfo("America/New_York")

FORECAST_WEATHER_SQL = """
    SELECT hour_start, temperature_c, precipitation_mm
    FROM weather_hour
    WHERE is_forecast = true AND hour_start >= %s AND hour_start < %s
    ORDER BY hour_start
"""


def current_hour() -> datetime:
    """Current New York hour without tz, rounded down (spec: hour_start)."""
    now = datetime.now(NY).replace(tzinfo=None)
    return now.replace(minute=0, second=0, microsecond=0)


def _read_inputs(start: datetime, end: datetime) -> tuple[list[str], pl.DataFrame]:
    """Read all station codes and the forecast weather for [start, end)."""
    with get_conn() as conn:
        codes = [r[0] for r in conn.execute("SELECT station_code FROM dim_station")]
        rows = conn.execute(FORECAST_WEATHER_SQL, [start, end]).fetchall()
    weather = pl.DataFrame(
        rows,
        schema={
            "hour_start": pl.Datetime("us"),
            "temperature_c": pl.Float64,
            "precipitation_mm": pl.Float64,
        },
        orient="row",
    )
    return codes, weather


def predict_next_hours(n_hours: int = 24) -> int:
    """C3: predict pickups/dropoffs for the next n_hours and store them.

    Input: n_hours, counted from the current hour (now rounded down), which the
    risk view needs as H0. Reads artifacts/ (C2 models and station means),
    dim_station, and forecast weather (weather_hour, is_forecast = true).
    Output: number of rows written to forecast_hour (model_version poisson_v1).
    Stations without station means and hours without forecast weather are
    skipped. Predictions are Poisson means, so they are always >= 0.
    """
    start = current_hour()
    end = start + timedelta(hours=n_hours)

    means = pl.read_parquet(ARTIFACTS / "station_means.parquet")
    models = {
        target: joblib.load(ARTIFACTS / f"poisson_{target}.joblib")
        for target in ("pickups", "dropoffs")
    }
    codes, weather = _read_inputs(start, end)
    if not codes or weather.is_empty():
        return 0

    # Every station x every forecast hour; build_features drops stations
    # without training means (spec C1).
    grid = pl.DataFrame({"station_code": codes}).join(weather, how="cross")
    features = build_features(grid, means)
    if features.is_empty():
        return 0

    x = features.select(FEATURE_COLUMNS).to_numpy()
    preds = features.select("station_code", "hour_start").with_columns(
        pl.Series("pred_pickups", models["pickups"].predict(x)),
        pl.Series("pred_dropoffs", models["dropoffs"].predict(x)),
    )
    return write_forecasts(preds)


if __name__ == "__main__":
    print("rows written:", predict_next_hours())
