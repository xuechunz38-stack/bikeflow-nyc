"""C3: tests for predict_next_hours (needs the Postgres from compose or CI).

Uses tiny Poisson models trained on random data in a temp artifacts/ folder,
made-up stations (9991.xx), and forecast weather only for hours that have
none yet. Everything the test adds is deleted afterwards, so real data in
the dev database is not touched.
"""

from datetime import timedelta

import joblib
import numpy as np
import polars as pl
import pytest
from sklearn.linear_model import PoissonRegressor

from bikeflow.db import get_conn
from bikeflow.model.features import FEATURE_COLUMNS
from bikeflow.model.predict import current_hour, predict_next_hours

KNOWN = ["9991.01", "9991.02", "9991.03"]  # stations with training means
UNKNOWN = "9991.04"  # station without training means -> not predicted
CODES = [*KNOWN, UNKNOWN]
N_HOURS = 3


def _cleanup(conn, weather_hours: list) -> None:
    """Delete test forecasts, test stations and weather rows the test added."""
    conn.execute("DELETE FROM forecast_hour WHERE station_code = ANY(%s)", [CODES])
    conn.execute("DELETE FROM dim_station WHERE station_code = ANY(%s)", [CODES])
    if weather_hours:
        conn.execute(
            "DELETE FROM weather_hour WHERE is_forecast AND hour_start = ANY(%s)",
            [weather_hours],
        )


@pytest.fixture
def setup(tmp_path, monkeypatch):
    """C3: temp artifacts + test stations + forecast weather for the next hours.

    Output: the list of hours the test expects predictions for.
    """
    # Tiny models with the right 37-column input, saved where predict looks.
    monkeypatch.chdir(tmp_path)
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    rng = np.random.default_rng(0)
    x = rng.random((200, len(FEATURE_COLUMNS)))
    for target in ("pickups", "dropoffs"):
        model = PoissonRegressor(max_iter=100).fit(x, rng.poisson(2.0, 200))
        joblib.dump(model, artifacts / f"poisson_{target}.joblib")
    pl.DataFrame(
        {
            "station_code": KNOWN,
            "mean_pickups": [1.0, 2.0, 0.0],
            "mean_dropoffs": [1.0, 0.5, 3.0],
        }
    ).write_parquet(artifacts / "station_means.parquet")

    hours = [current_hour() + timedelta(hours=k) for k in range(N_HOURS)]
    added = []
    with get_conn() as conn:
        conn.autocommit = True
        _cleanup(conn, [])
        for i, code in enumerate(CODES):
            conn.execute(
                "INSERT INTO dim_station "
                "(station_code, gbfs_station_id, name, lat, lon, capacity) "
                "VALUES (%s, %s, %s, 40.75, -73.99, 30)",
                [code, f"test-c3-{i}", f"C3 Test {i}"],
            )
        # Only add forecast weather for hours that have none, so real rows stay.
        for h in hours:
            cur = conn.execute(
                "INSERT INTO weather_hour (hour_start, temperature_c, "
                "precipitation_mm, wind_speed_kmh, is_forecast) "
                "VALUES (%s, 10.0, 0.0, 5.0, true) ON CONFLICT DO NOTHING",
                [h],
            )
            if cur.rowcount:
                added.append(h)
    yield hours
    with get_conn() as conn:
        conn.autocommit = True
        _cleanup(conn, added)


def _test_forecasts() -> list[tuple]:
    """Read the forecasts written for the test stations."""
    with get_conn() as conn:
        return conn.execute(
            "SELECT station_code, hour_start, pred_pickups, pred_dropoffs "
            "FROM forecast_hour WHERE station_code = ANY(%s) "
            "AND model_version = 'poisson_v1'",
            [CODES],
        ).fetchall()


def test_predicts_current_and_next_hours(setup):
    """C3 acceptance: current and next hour are predicted for every known station."""
    hours = setup
    predict_next_hours(N_HOURS)
    rows = _test_forecasts()
    for h in hours[:2]:
        assert {r[0] for r in rows if r[1] == h} == set(KNOWN)


def test_predictions_non_negative(setup):
    """C3 acceptance: all predicted values are >= 0."""
    predict_next_hours(N_HOURS)
    rows = _test_forecasts()
    assert rows
    assert all(r[2] >= 0 and r[3] >= 0 for r in rows)


def test_unknown_station_not_predicted(setup):
    """C3: stations without training means get no forecast."""
    predict_next_hours(N_HOURS)
    assert UNKNOWN not in {r[0] for r in _test_forecasts()}


def test_rerun_does_not_duplicate(setup):
    """C3: running twice keeps one row per (station, hour, model_version)."""
    hours = setup
    predict_next_hours(N_HOURS)
    predict_next_hours(N_HOURS)
    rows = [r for r in _test_forecasts() if r[1] in hours]
    assert len(rows) == len(KNOWN) * N_HOURS
