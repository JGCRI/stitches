"""Tests for the Zenodo package-data installer.

The download itself needs the network, but the parts most likely to break --
URL resolution, directory creation, archive extraction, and error handling on a
bad response -- are exercised offline against a locally built zip file.
"""

import io
import os
import zipfile

import pytest
import requests

import stitches
import stitches.install_pkgdata as sd


@pytest.fixture
def fake_archive(tmp_path):
    """Build a zip file shaped like the real Zenodo data archive.

    Includes nested ``tas-data`` and ``temp-data`` members plus files that must be
    filtered out, so extraction behavior can be checked in full.
    """
    path = tmp_path / "data.zip"

    with zipfile.ZipFile(path, "w") as zipped:
        zipped.writestr("data/matching_archive.csv", "a,b\n1,2\n")
        zipped.writestr("data/pangeo_table.csv", "a,b\n3,4\n")
        zipped.writestr("data/tas-data/CanESM5_tas.csv", "year,value\n1850,1.0\n")
        zipped.writestr("data/tas-data/BCC-CSM2-MR_tas.csv", "year,value\n1850,2.0\n")
        zipped.writestr("data/temp-data/scratch.csv", "x\n1\n")
        zipped.writestr("data/example.nc", b"\x89HDF\r\n\x1a\n")
        # Members that must be ignored.
        zipped.writestr("data/README.md", "# not extracted\n")
        zipped.writestr("data/notes.txt", "not extracted\n")

    return path


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------


def test_current_version_has_registered_data_url():
    """The installed version has an entry in the URL registry.

    Guards against tagging a release without registering its dataset, which would
    silently downgrade users to the fallback archive.
    """
    assert stitches.__version__ in sd.InstallPackageData.DATA_VERSION_URLS


def test_resolve_url_returns_registered_url():
    """A known version resolves to its registered URL."""
    installer = sd.InstallPackageData()

    resolved = installer.resolve_url("0.13")

    assert resolved == sd.InstallPackageData.DATA_VERSION_URLS["0.13"]


def test_resolve_url_warns_for_unknown_version(caplog):
    """An unregistered version falls back but logs a warning.

    The original code printed a message built into a local variable that was
    discarded on some paths, so the mismatch could pass unnoticed.
    """
    installer = sd.InstallPackageData()

    with caplog.at_level("WARNING"):
        resolved = installer.resolve_url("99.99.99-does-not-exist")

    assert resolved == sd.InstallPackageData.DEFAULT_VERSION
    assert any("no data archive is registered" in r.message.lower() for r in caplog.records)


def test_default_version_is_a_string():
    """``DEFAULT_VERSION`` is a URL string."""
    assert isinstance(sd.InstallPackageData.DEFAULT_VERSION, str)
    assert sd.InstallPackageData.DEFAULT_VERSION.startswith("https://")


# ---------------------------------------------------------------------------
# Target directory
# ---------------------------------------------------------------------------


def test_target_directory_honors_explicit_dir(tmp_path):
    """An explicit ``data_dir`` is used verbatim."""
    installer = sd.InstallPackageData(data_dir=str(tmp_path))

    assert installer.target_directory() == str(tmp_path)


def test_target_directory_defaults_into_package():
    """With no ``data_dir``, the package's own data directory is used."""
    installer = sd.InstallPackageData()

    assert installer.target_directory().endswith(os.path.join("stitches", "data"))


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def test_extract_keeps_only_csv_and_nc(tmp_path, fake_archive):
    """Only ``.csv`` and ``.nc`` members are extracted."""
    target = tmp_path / "out"

    installer = sd.InstallPackageData(data_dir=str(target))
    written = installer.extract(str(fake_archive), str(target))

    extensions = {os.path.splitext(f)[-1] for f in written}
    assert extensions <= {".csv", ".nc"}
    assert not (target / "README.md").exists()
    assert not (target / "notes.txt").exists()


def test_extract_preserves_tas_data_subdirectory(tmp_path, fake_archive):
    """``tas-data`` members land in the ``tas-data`` subdirectory."""
    target = tmp_path / "out"

    installer = sd.InstallPackageData(data_dir=str(target))
    installer.extract(str(fake_archive), str(target))

    assert (target / "tas-data" / "CanESM5_tas.csv").is_file()
    assert (target / "tas-data" / "BCC-CSM2-MR_tas.csv").is_file()


def test_extract_preserves_temp_data_subdirectory(tmp_path, fake_archive):
    """``temp-data`` members land in the ``temp-data`` subdirectory.

    Regression test: the original code created ``temp-data`` but only re-nested
    members whose path contained ``tas-data``, so ``temp-data`` files were
    flattened into the top level and the directory stayed empty.
    """
    target = tmp_path / "out"

    installer = sd.InstallPackageData(data_dir=str(target))
    installer.extract(str(fake_archive), str(target))

    assert (target / "temp-data" / "scratch.csv").is_file()
    assert not (target / "scratch.csv").exists()


def test_extract_flattens_top_level_members(tmp_path, fake_archive):
    """Members outside a known subdirectory are written to the top level."""
    target = tmp_path / "out"

    installer = sd.InstallPackageData(data_dir=str(target))
    installer.extract(str(fake_archive), str(target))

    assert (target / "matching_archive.csv").is_file()
    assert (target / "pangeo_table.csv").is_file()
    assert (target / "example.nc").is_file()


def test_extract_content_is_intact(tmp_path, fake_archive):
    """Extracted file contents match what was archived."""
    target = tmp_path / "out"

    installer = sd.InstallPackageData(data_dir=str(target))
    installer.extract(str(fake_archive), str(target))

    assert (target / "matching_archive.csv").read_text() == "a,b\n1,2\n"


# ---------------------------------------------------------------------------
# Download error handling
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for a streaming ``requests`` response."""

    def __init__(self, payload=b"", status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {"content-length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def iter_content(self, chunk_size=1):
        stream = io.BytesIO(self._payload)
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                return
            yield chunk


def test_download_raises_on_http_error(tmp_path, monkeypatch):
    """An error status raises instead of writing an error page to disk.

    The original code never called ``raise_for_status``, so a 404 HTML body was
    written out and only failed later with a confusing "not a zip file" error.
    """
    monkeypatch.setattr(
        sd.requests, "get", lambda *a, **k: _FakeResponse(b"<html>404</html>", status=404)
    )

    installer = sd.InstallPackageData(data_dir=str(tmp_path))

    with pytest.raises(requests.HTTPError):
        installer.download("https://example.invalid/data.zip", str(tmp_path / "d.zip"))


def test_download_streams_payload_to_disk(tmp_path, monkeypatch):
    """The response body is written to the destination path."""
    payload = b"x" * 4096
    monkeypatch.setattr(sd.requests, "get", lambda *a, **k: _FakeResponse(payload))

    destination = tmp_path / "d.zip"
    installer = sd.InstallPackageData(data_dir=str(tmp_path))
    installer.download("https://example.invalid/data.zip", str(destination))

    assert destination.read_bytes() == payload


def test_download_passes_timeout(tmp_path, monkeypatch):
    """A timeout is supplied, so a hung server cannot block forever.

    The original call had no timeout at all.
    """
    seen = {}

    def _capture(url, **kwargs):
        seen.update(kwargs)
        return _FakeResponse(b"data")

    monkeypatch.setattr(sd.requests, "get", _capture)

    installer = sd.InstallPackageData(data_dir=str(tmp_path))
    installer.download("https://example.invalid/data.zip", str(tmp_path / "d.zip"))

    assert seen.get("timeout") is not None
    assert seen.get("stream") is True


def test_fetch_zenodo_reports_bad_archive(tmp_path, monkeypatch):
    """A non-zip download produces an actionable error message."""
    monkeypatch.setattr(
        sd.requests, "get", lambda *a, **k: _FakeResponse(b"not a zip file at all")
    )

    installer = sd.InstallPackageData(data_dir=str(tmp_path))

    with pytest.raises(RuntimeError, match="not a valid zip archive"):
        installer.fetch_zenodo()


def test_fetch_zenodo_creates_subdirectories(tmp_path, monkeypatch, fake_archive):
    """Required subdirectories are created, including missing parents.

    The original code used ``os.mkdir``, which raises if a parent is absent.
    """
    payload = fake_archive.read_bytes()
    monkeypatch.setattr(sd.requests, "get", lambda *a, **k: _FakeResponse(payload))

    target = tmp_path / "missing" / "parents" / "data"
    installer = sd.InstallPackageData(data_dir=str(target))

    installer.fetch_zenodo()

    assert (target / "tas-data").is_dir()
    assert (target / "temp-data").is_dir()
    assert (target / "tas-data" / "CanESM5_tas.csv").is_file()


def test_fetch_zenodo_is_idempotent(tmp_path, monkeypatch, fake_archive):
    """Running twice succeeds; existing directories are not an error."""
    payload = fake_archive.read_bytes()
    monkeypatch.setattr(sd.requests, "get", lambda *a, **k: _FakeResponse(payload))

    target = tmp_path / "data"
    installer = sd.InstallPackageData(data_dir=str(target))

    installer.fetch_zenodo()
    written = installer.fetch_zenodo()

    assert len(written) > 0


def test_install_package_data_returns_written_files(tmp_path, monkeypatch, fake_archive):
    """The module-level helper returns the list of files written.

    Previously it returned ``None``, giving callers no way to confirm what landed.
    """
    payload = fake_archive.read_bytes()
    monkeypatch.setattr(sd.requests, "get", lambda *a, **k: _FakeResponse(payload))

    written = sd.install_package_data(data_dir=str(tmp_path / "data"))

    assert isinstance(written, list)
    assert len(written) == 6
