"""C1: tests for bikeflow.model.features (no database, no network)."""

import math
from datetime import datetime

import polars as pl

from bikeflow.model.features import FEATURE_COLUMNS, build_features


def _ts(text: str) -> datetime:
    """Naive New York local timestamp (spec: no timezone) from ISO text."""
    return datetime.fromisoformat(text)


def _inputs() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Small hand-made inputs: 2 known stations + 1 unknown station."""
    df = pl.DataFrame(
        {
            "station_code": ["1000.01", "1000.01", "2000.02", "9999.99"],
            "hour_start": [
                _ts("2026-09-07T08:00"),  # Monday, Labor Day
                _ts("2026-09-12T23:00"),  # Saturday
                _ts("2026-09-08T00:00"),  # Tuesday
                _ts("2026-09-08T00:00"),
            ],
            "temperature_c": [20.0, 18.5, 22.0, 22.0],
            "precipitation_mm": [0.0, 1.2, 0.0, 0.0],
        }
    )
    means = pl.DataFrame(
        {
            "station_code": ["1000.01", "2000.02"],
            "mean_pickups": [2.0, 0.0],
            "mean_dropoffs": [3.0, 0.5],
        }
    )
    return df, means


def _row(out: pl.DataFrame, code: str, ts: datetime) -> dict:
    """Pick one output row by station and hour."""
    match = out.filter((pl.col("station_code") == code) & (pl.col("hour_start") == ts))
    assert match.height == 1
    return match.row(0, named=True)


def test_feature_columns_fixed():
    """C1: 37 feature columns in the spec order."""
    assert len(FEATURE_COLUMNS) == 37
    assert FEATURE_COLUMNS[0] == "hour_0" and FEATURE_COLUMNS[23] == "hour_23"
    assert FEATURE_COLUMNS[24] == "dow_0" and FEATURE_COLUMNS[30] == "dow_6"
    assert FEATURE_COLUMNS[31:] == [
        "is_holiday",
        "temperature_c",
        "precipitation_mm",
        "is_rain",
        "log_mean_pickups",
        "log_mean_dropoffs",
    ]


def test_output_columns_and_no_nulls():
    """C1 acceptance: output columns and no null values."""
    df, means = _inputs()
    out = build_features(df, means)
    assert out.columns == ["station_code", "hour_start", *FEATURE_COLUMNS]
    assert out.null_count().sum_horizontal().item() == 0


def test_one_hot_sums_equal_one():
    """C1 acceptance: each row has exactly one hour_* and one dow_* set."""
    df, means = _inputs()
    out = build_features(df, means)
    hour_sum = out.select(pl.sum_horizontal(FEATURE_COLUMNS[:24])).to_series()
    dow_sum = out.select(pl.sum_horizontal(FEATURE_COLUMNS[24:31])).to_series()
    assert hour_sum.to_list() == [1.0] * out.height
    assert dow_sum.to_list() == [1.0] * out.height


def test_unknown_station_dropped():
    """C1: stations missing from station_means are not predicted."""
    df, means = _inputs()
    out = build_features(df, means)
    assert "9999.99" not in out["station_code"].to_list()
    assert out.height == 3


def test_feature_values():
    """C1: hour/dow positions, holiday, rain flag and log means."""
    df, means = _inputs()
    out = build_features(df, means)

    labor_day = _row(out, "1000.01", _ts("2026-09-07T08:00"))
    assert labor_day["hour_8"] == 1.0 and labor_day["dow_0"] == 1.0
    assert labor_day["is_holiday"] == 1.0
    assert labor_day["is_rain"] == 0.0
    assert math.isclose(labor_day["log_mean_pickups"], math.log(2.1))
    assert math.isclose(labor_day["log_mean_dropoffs"], math.log(3.1))

    saturday = _row(out, "1000.01", _ts("2026-09-12T23:00"))
    assert saturday["hour_23"] == 1.0 and saturday["dow_5"] == 1.0
    assert saturday["is_holiday"] == 0.0
    assert saturday["is_rain"] == 1.0

    zero_mean = _row(out, "2000.02", _ts("2026-09-08T00:00"))
    assert zero_mean["dow_1"] == 1.0
    assert math.isclose(zero_mean["log_mean_pickups"], math.log(0.1))


def test_missing_weather_rows_dropped():
    """C1: rows without weather are dropped so the output has no nulls."""
    df, means = _inputs()
    df = df.with_columns(
        pl.when(pl.col("hour_start") == _ts("2026-09-08T00:00"))
        .then(None)
        .otherwise(pl.col("temperature_c"))
        .alias("temperature_c")
    )
    out = build_features(df, means)
    assert out.height == 2
    assert out.null_count().sum_horizontal().item() == 0
