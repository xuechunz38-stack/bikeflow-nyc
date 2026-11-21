"""A4: station x hour aggregation (file F2)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from bikeflow.transform.clean import _month_bounds, clean_trips

STATION_HOUR_DIR = Path("data/processed/station_hour")


def station_hour_from_trips(trips: pl.DataFrame, month: str) -> pl.DataFrame:
    """A4: turn cleaned trips (output of clean_trips) into the F2 table.

    Input: cleaned trips for one month; month like "2026-09".
    Output: DataFrame station_code, hour_start, pickups, dropoffs with one row
    per (station seen this month) x (hour of this month); empty cells are 0.
    A station counts as seen if it has at least one pickup or dropoff counted
    below. Dropoffs whose ended_at falls in the next month are dropped.
    """
    start, end = _month_bounds(month)

    pickups = (
        trips.filter(pl.col("start_code").is_not_null())
        .group_by(
            pl.col("start_code").alias("station_code"),
            pl.col("started_at").dt.truncate("1h").alias("hour_start"),
        )
        .agg(pl.len().cast(pl.Int32).alias("pickups"))
    )
    dropoffs = (
        trips.filter(pl.col("end_code").is_not_null() & (pl.col("ended_at") < end))
        .group_by(
            pl.col("end_code").alias("station_code"),
            pl.col("ended_at").dt.truncate("1h").alias("hour_start"),
        )
        .agg(pl.len().cast(pl.Int32).alias("dropoffs"))
    )

    station_codes = pl.concat(
        [pickups.select("station_code"), dropoffs.select("station_code")]
    ).unique()
    hours = pl.DataFrame(
        {
            "hour_start": pl.datetime_range(
                start, end - timedelta(hours=1), "1h", time_unit="us", eager=True
            )
        }
    )
    grid = station_codes.join(hours, how="cross")

    return (
        grid.join(pickups, on=["station_code", "hour_start"], how="left")
        .join(dropoffs, on=["station_code", "hour_start"], how="left")
        .with_columns(pl.col("pickups", "dropoffs").fill_null(0).cast(pl.Int32))
        .select("station_code", "hour_start", "pickups", "dropoffs")
        .sort("station_code", "hour_start")
    )


def build_station_hour(month: str) -> Path:
    """A4: build F2 for one month.

    Input: month like "2026-09" (runs clean_trips on data/raw/trips/YYYYMM/).
    Output: path to data/processed/station_hour/station_hour_YYYYMM.parquet.
    """
    df = station_hour_from_trips(clean_trips(month), month)
    yyyymm = datetime.strptime(month, "%Y-%m").strftime("%Y%m")
    STATION_HOUR_DIR.mkdir(parents=True, exist_ok=True)
    path = STATION_HOUR_DIR / f"station_hour_{yyyymm}.parquet"
    df.write_parquet(path)
    return path
