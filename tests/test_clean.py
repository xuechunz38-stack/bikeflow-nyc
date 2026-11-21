"""Tests for A3 clean_trips, using tests/fixtures only (no network)."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from bikeflow.transform import clean

FIX = Path(__file__).parent / "fixtures"

CODE_CASES = [
    ("5980.10", "5980.10"),
    ("5980.1", "5980.10"),
    ("5303.06_", "5303.06"),
    ("5348.06_old", "5348.06"),
    ("5980.123", None),
    ("SYS016", None),
    ("Shop Morgan", None),
    ("", None),
    (None, None),
    ("  5980.1 ", "5980.10"),
    ("12.3_", "12.30"),
    ("5980.", None),
    (".10", None),
]


@pytest.mark.parametrize("raw, expected", CODE_CASES)
def test_normalize_station_code(raw, expected):
    assert clean.normalize_station_code(raw) == expected


def test_vectorized_normalizer_matches_python():
    df = pl.DataFrame({"x": [r for r, _ in CODE_CASES]}, schema={"x": pl.Utf8})
    got = df.select(clean.normalize_station_code_expr("x"))["x"].to_list()
    assert got == [e for _, e in CODE_CASES]


def _write_csv(raw: Path, name: str, rows: list[dict]):
    header = pl.read_csv(FIX / "trips_sample.csv", n_rows=0).columns
    full = [{c: r.get(c, "") for c in header} for r in rows]
    pl.DataFrame(full, schema={c: pl.Utf8 for c in header}).write_csv(raw / name)


def test_output_columns_and_types(workdir):
    df = clean.clean_trips("2026-09")
    assert df.schema == pl.Schema(
        {
            "ride_id": pl.Utf8,
            "started_at": pl.Datetime("us"),
            "ended_at": pl.Datetime("us"),
            "start_code": pl.Utf8,
            "end_code": pl.Utf8,
        }
    )
    assert df["ride_id"].is_unique().all()


def test_code_cases_in_fixture(workdir):
    df = clean.clean_trips("2026-09")
    got = dict(
        df.filter(pl.col("ride_id").str.starts_with("FIXTURE_CODE"))
        .select("ride_id", "start_code")
        .rows()
    )
    assert got == {
        "FIXTURE_CODE_01": "5980.10",
        "FIXTURE_CODE_02": "5980.10",
        "FIXTURE_CODE_03": "5303.06",
        "FIXTURE_CODE_04": "5348.06",
        "FIXTURE_CODE_05": None,
        "FIXTURE_CODE_06": None,
        "FIXTURE_CODE_07": None,
        "FIXTURE_CODE_08": None,
    }


def test_duration_outliers_dropped(workdir):
    ids = set(clean.clean_trips("2026-09")["ride_id"])
    assert "FIXTURE_DUR_SHORT" not in ids  # 0.5 minutes
    assert "FIXTURE_DUR_LONG" not in ids  # 1500 minutes


def test_codes_not_in_f1_become_null_but_row_kept(workdir):
    _write_csv(workdir, "extra.csv", [
        {"ride_id": "X1", "started_at": "2026-09-10 10:00:00", "ended_at": "2026-09-10 10:20:00",
         "start_station_id": "9999.99", "end_station_id": "6560.01"},
    ])
    row = clean.clean_trips("2026-09").filter(pl.col("ride_id") == "X1")
    assert row.select("start_code", "end_code").rows() == [(None, "6560.01")]


def test_dedup_month_filter_and_duration_bounds(workdir):
    _write_csv(workdir, "extra.csv", [
        {"ride_id": "D1", "started_at": "2026-09-10 10:00:00", "ended_at": "2026-09-10 10:01:00"},
        {"ride_id": "D1", "started_at": "2026-09-10 10:00:00", "ended_at": "2026-09-10 10:05:00"},
        {"ride_id": "EDGE_1440", "started_at": "2026-09-10 10:00:00",
         "ended_at": "2026-09-11 10:00:00"},
        {"ride_id": "AUG", "started_at": "2026-08-31 23:59:00", "ended_at": "2026-09-01 00:10:00"},
        {"ride_id": "OCT", "started_at": "2026-10-01 00:00:00", "ended_at": "2026-10-01 00:10:00"},
        {"ride_id": "INTO_OCT", "started_at": "2026-09-30 23:55:00",
         "ended_at": "2026-10-01 00:10:00"},
        {"ride_id": "BAD_TS", "started_at": "not a time", "ended_at": "2026-09-10 10:05:00"},
    ])
    df = clean.clean_trips("2026-09")
    ids = df["ride_id"].to_list()
    assert ids.count("D1") == 1
    assert {"EDGE_1440", "INTO_OCT"} <= set(ids)  # 1440 min is valid; month is by started_at
    assert not {"AUG", "OCT", "BAD_TS"} & set(ids)
    d1 = df.filter(pl.col("ride_id") == "D1")["ended_at"][0]
    assert d1 == datetime(2026, 9, 10, 10, 1)


def test_missing_folder_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        clean.clean_trips("2026-07")
