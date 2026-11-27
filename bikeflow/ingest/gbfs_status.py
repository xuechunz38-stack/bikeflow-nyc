"""B4: live GBFS station_status into the station_status table."""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import requests

from bikeflow.db import get_conn

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
NY = "America/New_York"

STATUS_SCHEMA = {
    "gbfs_station_id": pl.Utf8,
    "last_reported": pl.Datetime("us"),
    "fetched_at": pl.Datetime("us"),
    "num_bikes_available": pl.Int32,
    "num_ebikes_available": pl.Int32,
    "num_docks_available": pl.Int32,
    "is_renting": pl.Boolean,
    "is_returning": pl.Boolean,
}
STATUS_COLUMNS = list(STATUS_SCHEMA)


def ny_now() -> datetime:
    """B4: current New York local time without tz (spec section 2, 时间)."""
    return datetime.now(ZoneInfo(NY)).replace(tzinfo=None, microsecond=0)


def unix_to_ny(col: str) -> pl.Expr:
    """B4: Unix seconds (UTC) -> New York local time without tz (spec section 2)."""
    return (
        pl.from_epoch(pl.col(col).cast(pl.Int64), time_unit="s")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone(NY)
        .dt.replace_time_zone(None)
        .cast(pl.Datetime("us"))
    )


def status_from_gbfs(payload: dict, fetched_at: datetime) -> pl.DataFrame:
    """B4: turn a station_status.json payload into station_status rows.

    Input: parsed GBFS JSON; fetched_at as New York local time without tz.
    Output: DataFrame with the station_status columns (spec 4.2).
    Flags come as 0/1 and become booleans; missing flags become false.
    Stations without last_reported are dropped (it is part of the primary key).
    """
    rows = [
        {
            "gbfs_station_id": s["station_id"],
            "last_reported": s.get("last_reported"),
            "num_bikes_available": s["num_bikes_available"],
            "num_ebikes_available": s.get("num_ebikes_available"),
            "num_docks_available": s["num_docks_available"],
            "is_renting": bool(s.get("is_renting") or False),
            "is_returning": bool(s.get("is_returning") or False),
        }
        for s in payload["data"]["stations"]
    ]
    df = pl.DataFrame(
        rows,
        schema={
            "gbfs_station_id": pl.Utf8,
            "last_reported": pl.Int64,
            "num_bikes_available": pl.Int32,
            "num_ebikes_available": pl.Int32,
            "num_docks_available": pl.Int32,
            "is_renting": pl.Boolean,
            "is_returning": pl.Boolean,
        },
    )
    return (
        df.filter(pl.col("last_reported").is_not_null())
        .with_columns(
            unix_to_ny("last_reported").alias("last_reported"),
            pl.lit(fetched_at).cast(pl.Datetime("us")).alias("fetched_at"),
        )
        .select(STATUS_COLUMNS)
    )


def status_from_snapshot(path: str | Path) -> pl.DataFrame:
    """B4: read one B0 snapshot (data/station_status/YYYY-MM-DD/HHMMSS.csv.gz).

    Input: path to a gzipped CSV written by the B0 collector, with last_reported
    in Unix seconds, flags as 0/1 and fetched_at in UTC (ISO 8601).
    Output: DataFrame with the station_status columns, times in New York local
    time without tz. Rows without last_reported are dropped.
    """
    raw = pl.read_csv(
        path,
        schema_overrides={
            "station_id": pl.Utf8,
            "last_reported": pl.Int64,
            "num_bikes_available": pl.Int32,
            "num_ebikes_available": pl.Int32,
            "num_docks_available": pl.Int32,
            "is_renting": pl.Int8,
            "is_returning": pl.Int8,
            "fetched_at": pl.Utf8,
        },
    )
    return (
        raw.filter(pl.col("last_reported").is_not_null())
        .with_columns(
            pl.col("station_id").alias("gbfs_station_id"),
            unix_to_ny("last_reported").alias("last_reported"),
            pl.col("fetched_at")
            .str.to_datetime(time_zone="UTC")
            .dt.convert_time_zone(NY)
            .dt.replace_time_zone(None)
            .cast(pl.Datetime("us"))
            .alias("fetched_at"),
            (pl.col("is_renting").fill_null(0) == 1).alias("is_renting"),
            (pl.col("is_returning").fill_null(0) == 1).alias("is_returning"),
        )
        .select(STATUS_COLUMNS)
    )


def load_status_frame(df: pl.DataFrame) -> int:
    """B4: upsert station_status rows; on a key conflict only fetched_at changes.

    Input: DataFrame with STATUS_COLUMNS. Used by fetch_and_load_status and
    scripts/backfill_status.py.
    Output: number of rows written. Re-running never creates duplicates.
    """
    df = df.select(STATUS_COLUMNS).unique(
        subset=["gbfs_station_id", "last_reported"], keep="last"
    )
    cols = ", ".join(STATUS_COLUMNS)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_status (LIKE station_status) ON COMMIT DROP")
        with cur.copy(f"COPY tmp_status ({cols}) FROM STDIN (FORMAT csv)") as copy:
            buf = io.BytesIO()
            df.write_csv(buf, include_header=False)
            copy.write(buf.getvalue())
        cur.execute(
            f"INSERT INTO station_status ({cols}) SELECT {cols} FROM tmp_status "
            "ON CONFLICT (gbfs_station_id, last_reported) "
            "DO UPDATE SET fetched_at = EXCLUDED.fetched_at"
        )
        return cur.rowcount


def fetch_and_load_status() -> int:
    """B4: fetch GBFS station_status once and write it to station_status.

    Input: none (reads STATION_STATUS_URL).
    Output: number of rows written.
    """
    resp = requests.get(STATION_STATUS_URL, timeout=60)
    resp.raise_for_status()
    df = status_from_gbfs(resp.json(), fetched_at=ny_now())
    return load_status_frame(df)
