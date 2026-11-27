"""B4: load the B0 GBFS status history into station_status (`make backfill`).

Downloads the collector repo as a zip (no git needed), reads every
data/station_status/YYYY-MM-DD/HHMMSS.csv.gz snapshot and upserts it.
Safe to re-run: existing rows only get fetched_at updated.

Usage:
    python scripts/backfill_status.py                 # download from GitHub
    python scripts/backfill_status.py --source DIR    # use a local copy of the repo
"""

from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path

import polars as pl
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bikeflow.ingest.gbfs_status import load_status_frame, status_from_snapshot

COLLECTOR_ZIP = (
    "https://github.com/Zzqc-chien/bikeflow-gbfs-collector/archive/refs/heads/main.zip"
)
DOWNLOAD_DIR = Path("data/raw/gbfs_collector")


def download_collector(dest: Path = DOWNLOAD_DIR) -> Path:
    """B4: download and unzip the collector repo; return the repo root folder."""
    resp = requests.get(COLLECTOR_ZIP, timeout=300)
    resp.raise_for_status()
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(dest)
    return next(p for p in dest.iterdir() if p.is_dir())


def backfill(source: Path) -> int:
    """B4: load every snapshot under source/data/station_status; return rows written."""
    files = sorted((source / "data" / "station_status").glob("*/*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"no snapshots under {source}/data/station_status")
    df = pl.concat([status_from_snapshot(f) for f in files])
    rows = load_status_frame(df)
    print(f"{len(files)} snapshots, {df.height} rows read, {rows} rows written")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, help="local copy of the collector repo")
    args = parser.parse_args()
    backfill(args.source or download_collector())


if __name__ == "__main__":
    main()
