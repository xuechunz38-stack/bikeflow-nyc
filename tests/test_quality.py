"""Tests for A5 run_checks, using tests/fixtures only (no network)."""

import json
from pathlib import Path

import polars as pl
import pytest

from bikeflow.quality import checks
from bikeflow.transform.aggregate import build_station_hour

KEYS = [
    "month", "raw_rows", "valid_rows", "dropped_by_duration",
    "start_missing_rate", "start_matched_rate", "end_missing_rate", "end_matched_rate",
    "top_unmatched_codes", "n_stations", "grid_rows", "sum_pickups", "sum_dropoffs", "passed",
]
F5 = Path("data/processed/quality/quality_202609.json")


def test_fixture_month_passes_with_exact_keys(workdir):
    build_station_hour("2026-09")
    q = checks.run_checks("2026-09", min_raw_rows=0)
    assert list(q) == KEYS
    assert json.loads(F5.read_text()) == q
    assert q["passed"] is True
    assert q["raw_rows"] == 2010
    assert q["dropped_by_duration"] >= 2  # the two FIXTURE_DUR rows
    assert q["valid_rows"] == q["raw_rows"] - q["dropped_by_duration"]
    assert q["grid_rows"] == q["n_stations"] * 720
    # 4 hand-made code cases normalize to null (5980.123, SYS016, Shop Morgan,
    # empty); the real rows add a few more, so this is a lower bound.
    assert q["start_missing_rate"] >= 4 / q["valid_rows"]
    codes = {c["code"] for c in q["top_unmatched_codes"]}
    assert codes and codes <= {"1234.56", "5137.12", "5886.13"}
    assert all(set(c) == {"code", "count"} for c in q["top_unmatched_codes"])


def _set_start_ids(raw: Path, value: str, n: int):
    df = pl.read_csv(raw, infer_schema=False).with_row_index()
    df = df.with_columns(
        pl.when(pl.col("index") < n).then(pl.lit(value)).otherwise("start_station_id")
        .alias("start_station_id")
    ).drop("index")
    df.write_csv(raw)


def test_missing_rate_fails_and_still_writes_file(workdir):
    _set_start_ids(workdir / "trips_sample.csv", "SYS016", 100)  # ~5% missing
    build_station_hour("2026-09")
    with pytest.raises(ValueError, match="start_missing_rate"):
        checks.run_checks("2026-09", min_raw_rows=0)
    assert json.loads(F5.read_text())["passed"] is False


def test_matched_rate_fails(workdir):
    _set_start_ids(workdir / "trips_sample.csv", "9999.99", 300)  # ~15% unmatched
    build_station_hour("2026-09")
    with pytest.raises(ValueError, match="start_matched_rate"):
        checks.run_checks("2026-09", min_raw_rows=0)
    top = json.loads(F5.read_text())["top_unmatched_codes"][0]
    assert top == {"code": "9999.99", "count": 300}


def test_raw_row_bounds_fail(workdir):
    build_station_hour("2026-09")
    with pytest.raises(ValueError, match="raw_rows"):
        checks.run_checks("2026-09")  # default min is 2,000,000


def test_duplicate_pk_and_pickup_mismatch_fail(workdir):
    path = build_station_hour("2026-09")
    f2 = pl.read_parquet(path)
    extra = f2.filter(pl.col("pickups") > 0).head(1)
    pl.concat([f2, extra]).write_parquet(path)
    with pytest.raises(ValueError) as e:
        checks.run_checks("2026-09", min_raw_rows=0)
    assert "duplicate" in str(e.value) and "sum_pickups" in str(e.value)
