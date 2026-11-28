"""A1: download monthly Citi Bike NYC trip data."""

from __future__ import annotations

import io
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

import requests

BUCKET_URL = "https://s3.amazonaws.com/tripdata/"
RAW_TRIPS_DIR = Path("data/raw/trips")
_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")


def _month_to_yyyymm(month: str) -> str:
    m = _MONTH_RE.match(month)
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise ValueError(f"month must look like '2026-07', got {month!r}")
    return m.group(1) + m.group(2)


def _find_zip_key(yyyymm: str) -> str:
    """Return the NYC zip file name for this month from the bucket listing.

    The exact name varies between months, so we read the bucket listing
    instead of guessing. Files starting with 'JC-' (Jersey City) are skipped.
    """
    resp = requests.get(BUCKET_URL, timeout=60)
    resp.raise_for_status()
    keys = re.findall(r"<Key>([^<]+)</Key>", resp.text)
    matches = [
        k
        for k in keys
        if k.startswith(yyyymm) and "citibike-tripdata" in k and k.endswith(".zip")
    ]
    if not matches:
        raise FileNotFoundError(f"no NYC trip file for {yyyymm} in {BUCKET_URL}")
    if len(matches) > 1:
        raise RuntimeError(f"several NYC trip files for {yyyymm}: {matches}")
    return matches[0]


def _extract_csvs(zf: zipfile.ZipFile, out_dir: Path) -> None:
    """Copy every CSV in the zip (including CSVs in nested zips) into out_dir, flat."""
    for info in zf.infolist():
        name = Path(info.filename).name
        if info.is_dir() or name.startswith(".") or "__MACOSX" in info.filename:
            continue
        if name.lower().endswith(".csv"):
            with zf.open(info) as src, open(out_dir / name, "wb") as dst:
                shutil.copyfileobj(src, dst)
        elif name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(zf.read(info))) as inner:
                _extract_csvs(inner, out_dir)


def download_month(month: str) -> Path:
    """A1: download and unzip one month of NYC trip data.

    Input: month like "2026-07".
    Output: data/raw/trips/YYYYMM/ containing the month's CSV files.
    If that folder already has a CSV, nothing is downloaded.
    """
    yyyymm = _month_to_yyyymm(month)
    out_dir = RAW_TRIPS_DIR / yyyymm
    if out_dir.is_dir() and any(out_dir.glob("*.csv")):
        return out_dir

    key = _find_zip_key(yyyymm)
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    # Unzip into a temp folder first so a failed run never leaves half the CSVs
    # behind (which would make the next call skip the download).
    tmp_dir = Path(tempfile.mkdtemp(prefix=f".{yyyymm}_", dir=out_dir.parent))
    try:
        zip_path = tmp_dir / key
        with requests.get(BUCKET_URL + key, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with open(zip_path, "wb") as f:
                f.writelines(resp.iter_content(chunk_size=1 << 20))
        csv_dir = tmp_dir / "csv"
        csv_dir.mkdir()
        with zipfile.ZipFile(zip_path) as zf:
            _extract_csvs(zf, csv_dir)
        if not any(csv_dir.glob("*.csv")):
            raise RuntimeError(f"{key} contained no CSV files")
        if out_dir.exists():
            shutil.rmtree(out_dir)
        csv_dir.rename(out_dir)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return out_dir
