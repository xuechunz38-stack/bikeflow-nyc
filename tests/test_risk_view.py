"""C4: tests for the v_station_risk view (needs the Postgres from compose or CI).

The view compares against LOCALTIMESTAMP, which cannot be frozen, so every
test time is built relative to the database's current time. Test stations use
made-up codes (9990.xx) and are deleted before and after, so real data in the
dev database is not touched.
"""

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from bikeflow.db import get_conn

VIEWS_SQL = Path(__file__).resolve().parents[1] / "sql" / "views.sql"
MODEL = "poisson_v1"
CODES = [f"9990.{i:02d}" for i in range(1, 11)]
GBFS_IDS = [f"test-c4-{i:02d}" for i in range(1, 11)]

VIEW_COLUMNS = [
    "station_code",
    "name",
    "lat",
    "lon",
    "capacity",
    "snapshot_at",
    "bikes_now",
    "docks_now",
    "window_end",
    "exp_pickups",
    "exp_dropoffs",
    "projected_bikes",
    "projected_docks",
    "risk_level",
]


def _cleanup(conn) -> None:
    """Delete all C4 test rows from the three tables the view reads."""
    conn.execute("DELETE FROM forecast_hour WHERE station_code = ANY(%s)", [CODES])
    conn.execute(
        "DELETE FROM station_status WHERE gbfs_station_id = ANY(%s)", [GBFS_IDS]
    )
    conn.execute("DELETE FROM dim_station WHERE station_code = ANY(%s)", [CODES])


@pytest.fixture
def conn():
    """C4: connection with the view (re)created and test rows cleaned up.

    Output: an open psycopg connection; changes are committed per statement.
    """
    with get_conn() as c:
        c.autocommit = True
        c.execute(VIEWS_SQL.read_text())
        _cleanup(c)
        yield c
        _cleanup(c)


def _now(conn) -> datetime:
    """Return the database's LOCALTIMESTAMP (New York time, no tz)."""
    return conn.execute("SELECT LOCALTIMESTAMP").fetchone()[0]


def _hour(ts: datetime) -> datetime:
    """Round a timestamp down to the hour (hour_start)."""
    return ts.replace(minute=0, second=0, microsecond=0)


def _latest_at_minute(now: datetime, minute: int) -> datetime:
    """Most recent time <= now whose minute equals `minute` (always < 60 min ago)."""
    candidate = _hour(now).replace(minute=minute)
    return candidate if candidate <= now else candidate - timedelta(hours=1)


def _add_station(conn, i: int) -> str:
    """Insert test station number i into dim_station; return its station_code."""
    conn.execute(
        "INSERT INTO dim_station "
        "(station_code, gbfs_station_id, name, lat, lon, capacity) "
        "VALUES (%s, %s, %s, 40.75, -73.99, 30)",
        [CODES[i - 1], GBFS_IDS[i - 1], f"C4 Test {i}"],
    )
    return CODES[i - 1]


def _add_snapshot(
    conn,
    i: int,
    last_reported: datetime,
    fetched_at: datetime,
    bikes: int,
    docks: int,
    is_renting: bool = True,
) -> None:
    """Insert one station_status row for test station number i."""
    conn.execute(
        "INSERT INTO station_status "
        "(gbfs_station_id, last_reported, fetched_at, num_bikes_available, "
        "num_docks_available, is_renting, is_returning) "
        "VALUES (%s, %s, %s, %s, %s, %s, true)",
        [GBFS_IDS[i - 1], last_reported, fetched_at, bikes, docks, is_renting],
    )


def _add_forecast(
    conn, code: str, hour_start: datetime, pickups: float, dropoffs: float
) -> None:
    """Insert one forecast_hour row (model poisson_v1)."""
    conn.execute(
        "INSERT INTO forecast_hour "
        "(station_code, hour_start, model_version, pred_pickups, pred_dropoffs, "
        "created_at) VALUES (%s, %s, %s, %s, %s, LOCALTIMESTAMP)",
        [code, hour_start, MODEL, pickups, dropoffs],
    )


def _add_h0_h1(conn, code: str, snapshot_at: datetime, p: float, d: float) -> None:
    """Insert the same forecast for H0 and H1 of a snapshot."""
    _add_forecast(conn, code, _hour(snapshot_at), p, d)
    _add_forecast(conn, code, _hour(snapshot_at) + timedelta(hours=1), p, d)


def _view_rows(conn) -> dict[str, dict]:
    """Read the test stations from v_station_risk, keyed by station_code."""
    cur = conn.execute(
        "SELECT * FROM v_station_risk WHERE station_code = ANY(%s)", [CODES]
    )
    names = [col.name for col in cur.description]
    return {row[0]: dict(zip(names, row)) for row in cur.fetchall()}


def _assert_no_data(row: dict) -> None:
    """no_data rows keep the first 5 columns; everything else is NULL."""
    assert row["risk_level"] == "no_data"
    assert row["name"] is not None
    for col in VIEW_COLUMNS[5:-1]:
        assert row[col] is None, col


def test_view_columns_in_spec_order(conn):
    """C4: the view exposes exactly the 4.2 columns, in order."""
    cur = conn.execute("SELECT * FROM v_station_risk LIMIT 0")
    assert [col.name for col in cur.description] == VIEW_COLUMNS


def test_seven_station_cases(conn):
    """C4 acceptance: 7 stations -> empty, full, ok and four no_data cases."""
    now = _now(conn)
    fresh = now - timedelta(minutes=5)

    # 1) few bikes, many pickups -> empty_risk
    s1 = _add_station(conn, 1)
    _add_snapshot(conn, 1, fresh, now, bikes=3, docks=27)
    _add_h0_h1(conn, s1, fresh, p=3, d=0)

    # 2) few docks, many dropoffs -> full_risk
    s2 = _add_station(conn, 2)
    _add_snapshot(conn, 2, fresh, now, bikes=27, docks=3)
    _add_h0_h1(conn, s2, fresh, p=0, d=3)

    # 3) plenty of both -> ok
    s3 = _add_station(conn, 3)
    _add_snapshot(conn, 3, fresh, now, bikes=15, docks=15)
    _add_h0_h1(conn, s3, fresh, p=1, d=1)

    # 4) snapshot fetched more than 30 minutes ago -> no_data
    s4 = _add_station(conn, 4)
    stale = now - timedelta(minutes=40)
    _add_snapshot(conn, 4, stale, stale, bikes=15, docks=15)
    _add_h0_h1(conn, s4, stale, p=1, d=1)

    # 5) is_renting = false -> no_data
    s5 = _add_station(conn, 5)
    _add_snapshot(conn, 5, fresh, now, bikes=15, docks=15, is_renting=False)
    _add_h0_h1(conn, s5, fresh, p=1, d=1)

    # 6) H1 forecast missing -> no_data
    s6 = _add_station(conn, 6)
    _add_snapshot(conn, 6, fresh, now, bikes=15, docks=15)
    _add_forecast(conn, s6, _hour(fresh), 1, 1)

    # 7) first-start case: snapshot in the previous hour (like 14:55 seen at
    #    15:05) but forecasts only exist from the current hour on -> no_data
    s7 = _add_station(conn, 7)
    prev_hour_snapshot = _hour(now) - timedelta(seconds=30)
    _add_snapshot(conn, 7, prev_hour_snapshot, now, bikes=15, docks=15)
    _add_forecast(conn, s7, _hour(now), 1, 1)
    _add_forecast(conn, s7, _hour(now) + timedelta(hours=1), 1, 1)

    rows = _view_rows(conn)
    assert rows[s1]["risk_level"] == "empty_risk"
    assert rows[s2]["risk_level"] == "full_risk"
    assert rows[s3]["risk_level"] == "ok"
    for code in (s4, s5, s6, s7):
        _assert_no_data(rows[code])


def test_r_is_two_thirds_at_minute_20(conn):
    """C4: a snapshot at XX:20 gives r = 2/3 (exp = 2/3 * H0 + H1)."""
    now = _now(conn)
    snap = _latest_at_minute(now, 20)
    code = _add_station(conn, 8)
    _add_snapshot(conn, 8, snap, now, bikes=3, docks=20)
    _add_forecast(conn, code, _hour(snap), 3, 0)
    _add_forecast(conn, code, _hour(snap) + timedelta(hours=1), 2, 1)

    row = _view_rows(conn)[code]
    assert row["exp_pickups"] == pytest.approx(2 / 3 * 3 + 2)
    assert row["exp_dropoffs"] == pytest.approx(2 / 3 * 0 + 1)
    assert row["projected_bikes"] == pytest.approx(0)
    assert row["risk_level"] == "empty_risk"


def test_window_end_for_55_and_05_snapshots(conn):
    """C4: window_end = snapshot hour + 2h (14:55 -> 16:00, 15:05 -> 17:00)."""
    now = _now(conn)
    cases = {9: _latest_at_minute(now, 55), 10: _latest_at_minute(now, 5)}
    codes = {}
    for i, snap in cases.items():
        codes[i] = _add_station(conn, i)
        _add_snapshot(conn, i, snap, now, bikes=15, docks=15)
        _add_h0_h1(conn, codes[i], snap, p=1, d=1)

    rows = _view_rows(conn)
    for i, snap in cases.items():
        assert rows[codes[i]]["window_end"] == _hour(snap) + timedelta(hours=2)


def test_every_station_has_one_row(conn):
    """C4: the view has exactly one row per dim_station station."""
    _add_station(conn, 1)
    n_view = conn.execute("SELECT count(*) FROM v_station_risk").fetchone()[0]
    n_dim = conn.execute("SELECT count(*) FROM dim_station").fetchone()[0]
    assert n_view == n_dim
