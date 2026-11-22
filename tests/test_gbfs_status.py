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
