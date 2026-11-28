"""Tests for A1 download_month. No network: requests.get is replaced by a fake."""

import io
import zipfile

import pytest

from bikeflow.ingest import trips

LISTING = """<ListBucketResult>
<Contents><Key>202607-citibike-tripdata.zip</Key></Contents>
<Contents><Key>JC-202607-citibike-tripdata.csv.zip</Key></Contents>
<Contents><Key>202608-citibike-tripdata.zip</Key></Contents>
</ListBucketResult>"""


def _make_zip() -> bytes:
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as z:
        z.writestr("202607-citibike-tripdata_2.csv", "ride_id\nb\n")
    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as z:
        z.writestr("202607-citibike-tripdata/202607-citibike-tripdata_1.csv", "ride_id\na\n")
        z.writestr("__MACOSX/._junk.csv", "x")
        z.writestr("nested.zip", inner.getvalue())
    return outer.getvalue()


class FakeResp:
    def __init__(self, text="", content=b""):
        self.text = text
        self.content = content

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self.content

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


@pytest.fixture
def fake_s3(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if url == trips.BUCKET_URL:
            return FakeResp(text=LISTING)
        return FakeResp(content=_make_zip())

    monkeypatch.setattr(trips.requests, "get", fake_get)
    return calls


def test_downloads_nyc_file_and_extracts_all_csvs(fake_s3):
    out = trips.download_month("2026-07")
    assert out.as_posix() == "data/raw/trips/202607"
    assert sorted(p.name for p in out.glob("*.csv")) == [
        "202607-citibike-tripdata_1.csv",
        "202607-citibike-tripdata_2.csv",
    ]
    assert fake_s3[-1].endswith("/202607-citibike-tripdata.zip")  # not the JC- file


def test_second_call_skips_download(fake_s3):
    trips.download_month("2026-07")
    n = len(fake_s3)
    trips.download_month("2026-07")
    assert len(fake_s3) == n


def test_missing_month_raises(fake_s3):
    with pytest.raises(FileNotFoundError):
        trips.download_month("2026-09")


@pytest.mark.parametrize("bad", ["202607", "2026-7", "2026-13", "July"])
def test_bad_month_format(bad):
    with pytest.raises(ValueError):
        trips.download_month(bad)
