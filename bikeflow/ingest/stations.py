"""A2: station table F1 from GBFS station_information."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import requests

from bikeflow.transform.clean import normalize_station_code

STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATIONS_PATH = Path("data/processed/stations.parquet")

F1_SCHEMA = {
    "station_code": pl.Utf8,
    "gbfs_station_id": pl.Utf8,
    "name": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "capacity": pl.Int32,
}


def stations_from_gbfs(payload: dict) -> pl.DataFrame:
    """A2: turn a station_information.json payload into the F1 table.

    Input: parsed JSON from GBFS station_information.
    Output: DataFrame with the F1 columns and types (spec 4.1).
    Stations whose short_name cannot be normalized are dropped.
    If several stations share a station_code, keep the one whose short_name
    is exactly that code with no suffix (team decision, e.g. 5303.06 is kept
    and 5303.06_ is dropped). Raises ValueError if that does not leave one.
    """
    rows = [
        {
            "station_code": normalize_station_code(s.get("short_name")),
            "gbfs_station_id": s["station_id"],
            "name": s["name"],
            "lat": s["lat"],
            "lon": s["lon"],
            "capacity": s["capacity"],
            "_raw": (s.get("short_name") or "").strip(),
        }
        for s in payload["data"]["stations"]
    ]
    df = pl.DataFrame(rows, schema={**F1_SCHEMA, "_raw": pl.Utf8}).filter(
        pl.col("station_code").is_not_null()
    )
    is_dup = pl.col("station_code").is_duplicated()
    exact = pl.col("_raw") == pl.col("station_code")
    bad = (
        df.filter(is_dup)
        .group_by("station_code")
        .agg(exact.sum().alias("n_exact"))
        .filter(pl.col("n_exact") != 1)
    )
    if bad.height:
        raise ValueError(
            "duplicate station_code with no single unsuffixed short_name: "
            f"{sorted(bad['station_code'])}"
        )
    return df.filter(~is_dup | exact).drop("_raw").sort("station_code")


def fetch_stations() -> Path:
    """A2: download GBFS station_information and write F1.

    Input: none (reads STATION_INFO_URL).
    Output: path to data/processed/stations.parquet.
    """
    resp = requests.get(STATION_INFO_URL, timeout=60)
    resp.raise_for_status()
    df = stations_from_gbfs(resp.json())
    STATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(STATIONS_PATH)
    return STATIONS_PATH
