"""Shared pytest fixtures. Everything comes from tests/fixtures (no network)."""

import json
import shutil
from pathlib import Path

import pytest

from bikeflow.ingest.stations import stations_from_gbfs

FIX = Path(__file__).parent / "fixtures"


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    """A temp repo root with the trip sample as 2026-09 raw data and F1 built
    from stations_sample.json. Returns the raw trips folder."""
    monkeypatch.chdir(tmp_path)
    raw = Path("data/raw/trips/202609")
    raw.mkdir(parents=True)
    shutil.copy(FIX / "trips_sample.csv", raw / "trips_sample.csv")
    st = stations_from_gbfs(json.loads((FIX / "stations_sample.json").read_text()))
    Path("data/processed").mkdir(parents=True)
    st.write_parquet("data/processed/stations.parquet")
    return raw
