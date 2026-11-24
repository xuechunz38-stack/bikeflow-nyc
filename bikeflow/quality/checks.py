"""A5: data quality checks for one month (file F5)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl

from bikeflow.transform.aggregate import STATION_HOUR_DIR
from bikeflow.transform.clean import (
    load_station_codes,
    month_trips,
    scan_raw_trips,
    valid_trips_normalized,
)

QUALITY_DIR = Path("data/processed/quality")
MIN_MATCHED_RATE = 0.90
MAX_MISSING_RATE = 0.02


def _rate(num: int, den: int) -> float:
    return num / den if den else 0.0


def run_checks(
    month: str, min_raw_rows: int = 2_000_000, max_raw_rows: int = 8_000_000
) -> dict:
    """A5: run the quality checks for one month and write F5.

    Input: month like "2026-09". Reads raw CSVs, F1 and F2 (so run it after
    build_station_hour). Tests with fixtures pass min_raw_rows=0.
    Output: dict with exactly the keys listed in spec A5, also written to
    data/processed/quality/quality_YYYYMM.json. If any check fails the file
    is still written with passed=false, then ValueError is raised.
    """
    yyyymm = datetime.strptime(month, "%Y-%m").strftime("%Y%m")
    known = load_station_codes().implode()

    raw_rows = scan_raw_trips(month).select(pl.len()).collect().item()
    in_month_rows = month_trips(month).select(pl.len()).collect().item()
    valid = valid_trips_normalized(month).collect()
    valid_rows = valid.height

    rates = {}
    for side in ("start", "end"):
        norm = valid[f"{side}_norm"]
        non_null = norm.drop_nulls()
        matched = non_null.is_in(known).sum()
        rates[f"{side}_missing_rate"] = _rate(norm.null_count(), valid_rows)
        rates[f"{side}_matched_rate"] = _rate(matched, non_null.len())

    unmatched = (
        pl.concat([valid["start_norm"], valid["end_norm"]])
        .drop_nulls()
        .alias("code")
        .to_frame()
        .filter(~pl.col("code").is_in(known))
        .group_by("code")
        .agg(pl.len().alias("count"))
        .sort(["count", "code"], descending=[True, False])
        .head(10)
    )
    expected_pickups = valid["start_norm"].is_in(known).sum()

    f2 = pl.read_parquet(STATION_HOUR_DIR / f"station_hour_{yyyymm}.parquet")
    f2_has_dup_pk = not f2.select("station_code", "hour_start").is_unique().all()
    sum_pickups = int(f2["pickups"].sum())

    failures = []
    if not min_raw_rows <= raw_rows <= max_raw_rows:
        failures.append(f"raw_rows {raw_rows} not in [{min_raw_rows}, {max_raw_rows}]")
    for side in ("start", "end"):
        if rates[f"{side}_matched_rate"] < MIN_MATCHED_RATE:
            failures.append(f"{side}_matched_rate < {MIN_MATCHED_RATE}")
        if rates[f"{side}_missing_rate"] > MAX_MISSING_RATE:
            failures.append(f"{side}_missing_rate > {MAX_MISSING_RATE}")
    if f2_has_dup_pk:
        failures.append("F2 has duplicate (station_code, hour_start)")
    if sum_pickups != expected_pickups:
        failures.append(f"sum_pickups {sum_pickups} != valid rides with start_code {expected_pickups}")

    result = {
        "month": month,
        "raw_rows": raw_rows,
        "valid_rows": valid_rows,
        "dropped_by_duration": in_month_rows - valid_rows,
        **rates,
        "top_unmatched_codes": unmatched.to_dicts(),
        "n_stations": f2["station_code"].n_unique(),
        "grid_rows": f2.height,
        "sum_pickups": sum_pickups,
        "sum_dropoffs": int(f2["dropoffs"].sum()),
        "passed": not failures,
    }
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    (QUALITY_DIR / f"quality_{yyyymm}.json").write_text(json.dumps(result, indent=2))
    if failures:
        raise ValueError(f"quality checks failed for {month}: " + "; ".join(failures))
    return result
