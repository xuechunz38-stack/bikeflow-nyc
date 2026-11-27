"""B3: load A's parquet files (F1, F2, F3/F4) into Postgres."""

from __future__ import annotations

import io
from pathlib import Path

import polars as pl

from bikeflow.db import get_conn

STATION_COLUMNS = ["station_code", "gbfs_station_id", "name", "lat", "lon", "capacity"]
STATION_HOUR_COLUMNS = ["station_code", "hour_start", "pickups", "dropoffs"]
WEATHER_COLUMNS = [
    "hour_start",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
    "is_forecast",
]
_BATCH_ROWS = 200_000


def _upsert(df: pl.DataFrame, table: str, key_cols: list[str]) -> int:
    """B3: COPY df into a temp table, then INSERT ... ON CONFLICT DO UPDATE.

    Input: DataFrame whose columns match the target table, table name,
    primary key columns.
    Output: number of rows written (inserted or updated).
    Runs in one transaction, so a failed load leaves the table unchanged.
    """
    cols = ", ".join(df.columns)
    keys = ", ".join(key_cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in df.columns if c not in key_cols)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE tmp_load (LIKE {table}) ON COMMIT DROP")
        with cur.copy(f"COPY tmp_load ({cols}) FROM STDIN (FORMAT csv)") as copy:
            for batch in df.iter_slices(_BATCH_ROWS):
                buf = io.BytesIO()
                batch.write_csv(buf, include_header=False)
                copy.write(buf.getvalue())
        cur.execute(
            f"INSERT INTO {table} ({cols}) SELECT {cols} FROM tmp_load "
            f"ON CONFLICT ({keys}) DO UPDATE SET {updates}"
        )
        return cur.rowcount


def load_stations(path: str | Path) -> int:
    """B3: load F1 (data/processed/stations.parquet) into dim_station.

    Input: path to the F1 parquet file.
    Output: number of rows written. Loading the same file twice keeps the row count.
    """
    df = pl.read_parquet(path).select(STATION_COLUMNS)
    return _upsert(df, "dim_station", ["station_code"])


def load_station_hour(path: str | Path) -> int:
    """B3: load one F2 file (station_hour_YYYYMM.parquet) into fact_station_hour.

    Input: path to an F2 parquet file. dim_station must be loaded first (FK).
    Output: number of rows written. Loading the same file twice keeps the row count.
    """
    df = pl.read_parquet(path).select(STATION_HOUR_COLUMNS)
    return _upsert(df, "fact_station_hour", ["station_code", "hour_start"])


def load_weather(path: str | Path, is_forecast: bool) -> int:
    """B3: load F3 (history) or F4 (forecast) into weather_hour.

    Input: path to the F3/F4 parquet file; is_forecast=False for F3, True for F4.
    Output: number of rows written. Loading the same file twice keeps the row count.
    """
    df = (
        pl.read_parquet(path)
        .with_columns(pl.lit(is_forecast).alias("is_forecast"))
        .select(WEATHER_COLUMNS)
    )
    return _upsert(df, "weather_hour", ["hour_start"])
