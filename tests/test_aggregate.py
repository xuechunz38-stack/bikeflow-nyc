"""Tests for A4 build_station_hour, using tests/fixtures only (no network)."""

from datetime import datetime

import polars as pl

from bikeflow.transform import aggregate, clean


def test_f2_file_grid_and_reconciliation(workdir):
    path = aggregate.build_station_hour("2026-09")
    assert path.as_posix() == "data/processed/station_hour/station_hour_202609.parquet"
    f2 = pl.read_parquet(path)
    assert f2.schema == pl.Schema(
        {
            "station_code": pl.Utf8,
            "hour_start": pl.Datetime("us"),
            "pickups": pl.Int32,
            "dropoffs": pl.Int32,
        }
    )
    n_stations = f2["station_code"].n_unique()
    assert f2.height == n_stations * 720  # September has 720 hours
    assert f2.select("station_code", "hour_start").is_unique().all()
    assert f2["hour_start"].min() == datetime(2026, 9, 1)
    assert f2["hour_start"].max() == datetime(2026, 9, 30, 23)

    trips = clean.clean_trips("2026-09")
    assert f2["pickups"].sum() == trips["start_code"].is_not_null().sum()
    f1 = set(pl.read_parquet("data/processed/stations.parquet")["station_code"])
    assert set(f2["station_code"]) <= f1


def _trip(ride_id, start, end, s, e):
    return {
        "ride_id": ride_id,
        "started_at": datetime.fromisoformat(start),
        "ended_at": datetime.fromisoformat(end),
        "start_code": s,
        "end_code": e,
    }


def test_counts_hours_zero_fill_and_next_month_dropoff():
    trips = pl.DataFrame([
        _trip("1", "2026-09-01 08:00:00", "2026-09-01 08:20:00", "A.01", "B.01"),
        _trip("2", "2026-09-01 08:59:59", "2026-09-01 09:10:00", "A.01", None),
        _trip("3", "2026-09-30 23:50:00", "2026-10-01 00:05:00", "B.01", "A.01"),
    ])
    f2 = aggregate.station_hour_from_trips(trips, "2026-09")
    assert f2.height == 2 * 720
    cell = {(r[0], r[1]): (r[2], r[3]) for r in f2.rows()}
    assert cell[("A.01", datetime(2026, 9, 1, 8))] == (2, 0)
    assert cell[("B.01", datetime(2026, 9, 1, 8))] == (0, 1)
    assert cell[("A.01", datetime(2026, 9, 1, 9))] == (0, 0)
    assert cell[("B.01", datetime(2026, 9, 30, 23))] == (1, 0)
    assert f2["dropoffs"].sum() == 1  # the October dropoff is dropped
