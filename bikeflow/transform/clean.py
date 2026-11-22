"""A3: trip cleaning."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import polars as pl

RAW_TRIPS_DIR = Path("data/raw/trips")
STATIONS_PATH = Path("data/processed/stations.parquet")

_CODE_PATTERN = r"^(\d+)\.(\d{1,2})(_.*)?$"
_CODE_RE = re.compile(_CODE_PATTERN)


def normalize_station_code(raw: str | None) -> str | None:
    """A3: normalize a station code to the form 1234.56 (spec section 2).

    Input: raw code from trips (start/end_station_id) or GBFS short_name.
    Output: normalized code, or None when the input does not match.
    Examples: "5980.1" -> "5980.10", "5348.06_old" -> "5348.06", "SYS016" -> None.
    """
    if raw is None:
        return None
    m = _CODE_RE.match(raw.strip())
    if not m:
        return None
    return f"{m.group(1)}.{m.group(2).ljust(2, '0')}"


def normalize_station_code_expr(col: str) -> pl.Expr:
    """A3: same rule as normalize_station_code, as a fast polars expression.

    Input: name of a string column. Output: expression giving the normalized
    code or null. Used by clean_trips so millions of rows stay vectorized.
    """
    s = pl.col(col).str.strip_chars()
    whole = s.str.extract(_CODE_PATTERN, 0)
    return (
        pl.when(whole.is_not_null())
        .then(
            s.str.extract(_CODE_PATTERN, 1)
            + "."
            + s.str.extract(_CODE_PATTERN, 2).str.pad_end(2, "0")
        )
        .otherwise(None)
    )


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(month, "%Y-%m")
    end = datetime(start.year + start.month // 12, start.month % 12 + 1, 1)
    return start, end


def _parse_ts(col: str) -> pl.Expr:
    # Source looks like "2026-09-05 18:45:47.450"; fractional seconds are optional.
    # Unparseable values become null and are dropped by the filters below.
    return (
        pl.col(col)
        .str.strip_chars()
        .str.to_datetime("%Y-%m-%d %H:%M:%S%.f", time_unit="us", strict=False)
    )


VALID_DURATION = (
    (pl.col("ended_at") - pl.col("started_at")).dt.total_milliseconds() / 60_000
).is_between(1, 1440)


def scan_raw_trips(month: str) -> pl.LazyFrame:
    """A3 step 1: lazily read every CSV in data/raw/trips/YYYYMM/, all columns as strings."""
    csv_dir = RAW_TRIPS_DIR / _month_bounds(month)[0].strftime("%Y%m")
    files = sorted(csv_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"no CSV files in {csv_dir}")
    return pl.concat(
        [pl.scan_csv(f, infer_schema=False) for f in files], how="diagonal_relaxed"
    )


def month_trips(month: str) -> pl.LazyFrame:
    """A3 steps 2-3: dedup by ride_id, parse times, keep started_at in the month.

    Output keeps the raw station id columns so A5 can measure missing rates.
    """
    start, end = _month_bounds(month)
    return (
        scan_raw_trips(month)
        .select("ride_id", "started_at", "ended_at", "start_station_id", "end_station_id")
        .unique(subset="ride_id", keep="first", maintain_order=True)
        .with_columns(_parse_ts("started_at"), _parse_ts("ended_at"))
        .filter((pl.col("started_at") >= start) & (pl.col("started_at") < end))
    )


def valid_trips_normalized(month: str) -> pl.LazyFrame:
    """A3 steps 4-5: valid rides with normalized codes, before the F1 check.

    Output: ride_id, started_at, ended_at, start_norm, end_norm.
    """
    return (
        month_trips(month)
        .filter(VALID_DURATION)
        .with_columns(
            normalize_station_code_expr("start_station_id").alias("start_norm"),
            normalize_station_code_expr("end_station_id").alias("end_norm"),
        )
        .select("ride_id", "started_at", "ended_at", "start_norm", "end_norm")
    )


def load_station_codes() -> pl.Series:
    """Station codes from F1 (data/processed/stations.parquet)."""
    return pl.read_parquet(STATIONS_PATH, columns=["station_code"])["station_code"]


def clean_trips(month: str) -> pl.DataFrame:
    """A3: clean one month of raw trips.

    Input: month like "2026-09"; reads every CSV in data/raw/trips/YYYYMM/ and
    the station table F1 (data/processed/stations.parquet).
    Output: DataFrame with ride_id, started_at, ended_at, start_code, end_code.
    Steps (spec A3, in order): read all columns as strings; drop duplicate
    ride_id; keep rows whose started_at is in the month; keep valid rides
    (1 to 1440 minutes); normalize station ids; set codes not in F1 to null.
    """
    lf = valid_trips_normalized(month)  # raises if the folder is empty
    known = load_station_codes().implode()
    return (
        lf.with_columns(
            pl.when(pl.col(f"{p}_norm").is_in(known)).then(pl.col(f"{p}_norm")).alias(f"{p}_code")
            for p in ("start", "end")
        )
        .select("ride_id", "started_at", "ended_at", "start_code", "end_code")
        .collect()
    )
