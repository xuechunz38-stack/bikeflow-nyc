"""B2: schema tests (needs the Postgres from compose or CI)."""

from bikeflow.db import get_conn

EXPECTED = {
    "dim_station",
    "fact_station_hour",
    "weather_hour",
    "station_status",
    "forecast_hour",
}


def test_five_tables_exist():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ).fetchall()
    assert EXPECTED <= {r[0] for r in rows}
