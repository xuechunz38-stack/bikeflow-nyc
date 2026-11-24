"""Tests for A2 (stations). No network. Normalizer tests live in test_clean.py."""

import polars as pl
import pytest

from bikeflow.ingest import stations


def _station(sid, short_name, cap=20):
    return {
        "station_id": sid,
        "short_name": short_name,
        "name": f"Station {sid}",
        "lat": 40.7,
        "lon": -74.0,
        "capacity": cap,
    }


def _payload(*stations_):
    return {"data": {"stations": list(stations_)}}


def test_stations_from_gbfs_schema_and_filtering():
    df = stations.stations_from_gbfs(
        _payload(_station("u1", "6560.01"), _station("u2", "5980.1"), _station("u3", "JC141"))
    )
    assert df.schema == pl.Schema(stations.F1_SCHEMA)
    assert df["station_code"].to_list() == ["5980.10", "6560.01"]
    assert df["station_code"].str.contains(r"^\d+\.\d{2}$").all()


def test_duplicate_code_keeps_unsuffixed_station():
    df = stations.stations_from_gbfs(
        _payload(_station("u1", "5303.06_", cap=15), _station("u2", "5303.06", cap=54))
    )
    assert df.select("station_code", "gbfs_station_id", "capacity").rows() == [
        ("5303.06", "u2", 54)
    ]


def test_duplicate_codes_without_single_unsuffixed_raise():
    with pytest.raises(ValueError, match="5980.10"):
        stations.stations_from_gbfs(_payload(_station("u1", "5980.1"), _station("u2", "5980.10_x")))


def test_fetch_stations_writes_parquet(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return _payload(_station("u1", "6560.01"))

    monkeypatch.setattr(stations.requests, "get", lambda url, **kw: Resp())
    path = stations.fetch_stations()
    assert path.as_posix() == "data/processed/stations.parquet"
    assert pl.read_parquet(path)["gbfs_station_id"].to_list() == ["u1"]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("5980.10", "5980.10"),
        ("5980.1", "5980.10"),
        ("5303.06_", "5303.06"),
        ("5348.06_old", "5348.06"),
        ("5980.123", None),
        ("SYS016", None),
        ("Shop Morgan", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_station_code(raw, expected):
    from bikeflow.transform.clean import normalize_station_code

    assert normalize_station_code(raw) == expected
