"""B4: GBFS status tests. No network; the load test needs Postgres."""

from datetime import datetime

import pytest

from bikeflow.db import get_conn
from bikeflow.ingest.gbfs_status import load_status_frame, status_from_gbfs

IDS = ["test-s1", "test-s2", "test-s3"]
FETCHED = datetime.fromisoformat("2026-11-21T14:30:00")

PAYLOAD = {
    "data": {
        "stations": [
            {
                # 2026-11-21 19:20 UTC = 14:20 New York (EST)
                "station_id": "test-s1",
                "last_reported": 1795288800,
                "num_bikes_available": 5,
                "num_ebikes_available": 2,
                "num_docks_available": 10,
                "is_renting": 1,
                "is_returning": 1,
            },
            {
                # 2026-07-15 16:00 UTC = 12:00 New York (EDT)
                "station_id": "test-s2",
                "last_reported": 1784131200,
                "num_bikes_available": 0,
                "num_docks_available": 20,
                "is_renting": 0,
                "is_returning": 1,
            },
            {
                "station_id": "test-s3",
                "num_bikes_available": 1,
                "num_docks_available": 1,
                "is_renting": 1,
                "is_returning": 1,
            },
        ]
    }
}


def test_status_from_gbfs_converts_times_and_flags():
    df = status_from_gbfs(PAYLOAD, fetched_at=FETCHED)
    assert df["gbfs_station_id"].to_list() == ["test-s1", "test-s2"]  # s3 has no time
    assert df["last_reported"].to_list() == [
        datetime.fromisoformat("2026-11-21T14:20:00"),
        datetime.fromisoformat("2026-07-15T12:00:00"),
    ]
    assert df["last_reported"].dtype.time_zone is None
    assert df["is_renting"].to_list() == [True, False]
    assert df["num_ebikes_available"].to_list() == [2, None]
    assert df["fetched_at"].to_list() == [FETCHED, FETCHED]


@pytest.fixture
def clean_status():
    def _cleanup():
        with get_conn() as conn:
            conn.execute(
                "DELETE FROM station_status WHERE gbfs_station_id = ANY(%s)", [IDS]
            )

    _cleanup()
    yield
    _cleanup()


def test_load_twice_only_updates_fetched_at(clean_status):
    first = status_from_gbfs(PAYLOAD, fetched_at=FETCHED)
    later = datetime.fromisoformat("2026-11-21T14:45:00")
    second = status_from_gbfs(PAYLOAD, fetched_at=later)
    assert load_status_frame(first) == 2
    assert load_status_frame(second) == 2
    with get_conn() as conn:
        n, max_fetched = conn.execute(
            "SELECT count(*), max(fetched_at) FROM station_status "
            "WHERE gbfs_station_id = ANY(%s)",
            [IDS],
        ).fetchone()
    assert (n, max_fetched) == (2, later)


SNAPSHOT_CSV = (
    "station_id,last_reported,num_bikes_available,num_ebikes_available,"
    "num_docks_available,is_renting,is_returning,fetched_at\n"
    "test-s1,1795288800,5,2,10,1,1,2026-11-21T19:30:00+00:00\n"
    "test-s2,1784131200,0,,20,0,1,2026-11-21T19:30:00+00:00\n"
    "test-s3,,1,1,1,1,1,2026-11-21T19:30:00+00:00\n"
)


def _write_snapshot(root, day: str, name: str) -> None:
    import gzip

    d = root / "data" / "station_status" / day
    d.mkdir(parents=True, exist_ok=True)
    with gzip.open(d / name, "wt") as f:
        f.write(SNAPSHOT_CSV)


def test_status_from_snapshot_converts_utc_to_new_york(tmp_path):
    from bikeflow.ingest.gbfs_status import status_from_snapshot

    _write_snapshot(tmp_path, "2026-11-21", "193000.csv.gz")
    df = status_from_snapshot(tmp_path / "data/station_status/2026-11-21/193000.csv.gz")
    assert df["gbfs_station_id"].to_list() == ["test-s1", "test-s2"]
    assert df["last_reported"].to_list() == [
        datetime.fromisoformat("2026-11-21T14:20:00"),
        datetime.fromisoformat("2026-07-15T12:00:00"),
    ]
    # 19:30 UTC = 14:30 New York (EST), stored without tz
    assert df["fetched_at"].to_list() == [FETCHED, FETCHED]
    assert df["fetched_at"].dtype.time_zone is None
    assert df["is_renting"].to_list() == [True, False]
    assert df["num_ebikes_available"].to_list() == [2, None]


def test_backfill_twice_keeps_count(tmp_path, clean_status):
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "backfill_status",
        Path(__file__).resolve().parents[1] / "scripts" / "backfill_status.py",
    )
    backfill_status = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(backfill_status)

    # two snapshots with the same keys: still 2 rows, not 4
    _write_snapshot(tmp_path, "2026-11-21", "193000.csv.gz")
    _write_snapshot(tmp_path, "2026-11-22", "010000.csv.gz")
    backfill_status.backfill(tmp_path)
    backfill_status.backfill(tmp_path)
    with get_conn() as conn:
        n = conn.execute(
            "SELECT count(*) FROM station_status WHERE gbfs_station_id = ANY(%s)", [IDS]
        ).fetchone()[0]
    assert n == 2
