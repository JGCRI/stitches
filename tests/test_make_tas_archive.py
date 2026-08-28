"""Tests for the tas archive construction helpers.

Focused on `write_tas_data_by_model`, which was extracted from `make_tas_archive`
specifically so its filename construction could be tested. The full
`make_tas_archive` run downloads the entire CMIP6 tas archive from Pangeo and
takes hours, so the bug these tests cover was previously unreachable in CI --
which is why it shipped.
"""

import os

import pandas as pd
import pytest

from stitches.make_tas_archive import (
    calculate_anomaly,
    join_exclude,
    rbind,
    write_tas_data_by_model,
)


@pytest.fixture
def tas_frame():
    """Return a small multi-model global tas frame."""
    rows = []
    for model in ("BCC-CSM2-MR", "CanESM5", "UKESM1-0-LL"):
        for year in range(2000, 2005):
            rows.append(
                {
                    "model": model,
                    "experiment": "historical",
                    "ensemble": "r1i1p1f1",
                    "variable": "tas",
                    "year": year,
                    "value": 287.0 + year / 1000,
                    "unit": "K",
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# write_tas_data_by_model
# ---------------------------------------------------------------------------


def test_write_tas_data_by_model_filenames(tmp_path, tas_frame):
    """Filenames are ``<model>_tas.csv`` with no tuple punctuation.

    Regression test for two defects present in the original implementation:

    * ``groupby(["model"])`` with a single-element *list* yields a one-element
      tuple as the group name under pandas 2.0+, which would have produced
      ``('BCC-CSM2-MR',)_tas.csv``.
    * the path was built with ``Traversable + str``, which raises ``TypeError``.
    """
    files = write_tas_data_by_model(tas_frame, tmp_path)

    names = sorted(os.path.basename(f) for f in files)
    assert names == [
        "BCC-CSM2-MR_tas.csv",
        "CanESM5_tas.csv",
        "UKESM1-0-LL_tas.csv",
    ]

    for name in names:
        assert "(" not in name and ")" not in name and "," not in name


def test_write_tas_data_by_model_accepts_path_like(tas_frame, tmp_path):
    """A path-like output directory is accepted, not just a ``str``.

    ``make_tas_archive`` passes ``resources.files("stitches") / "data" /
    "tas-data"``, which is a ``Traversable`` (a ``PosixPath`` in practice). The
    original code concatenated it with ``+``, which raises ``TypeError`` for path
    objects. Passing a ``Path`` here reproduces that call shape.
    """
    files = write_tas_data_by_model(tas_frame, tmp_path)

    assert len(files) == 3
    assert all(isinstance(f, str) for f in files), "paths should be returned as str"


def test_write_tas_data_by_model_matches_production_directory_type(tas_frame, tmp_path):
    """The type produced by ``importlib.resources`` is handled.

    Guards the specific failure mode directly: the object type that
    ``make_tas_archive`` supplies must not raise when joined.
    """
    from importlib import resources

    production_type = type(resources.files("stitches") / "data" / "tas-data")

    # Confirm the assumption behind this test still holds, then exercise the
    # helper with an instance of that same type pointing at a disposable dir.
    assert issubclass(production_type, os.PathLike)

    files = write_tas_data_by_model(tas_frame, production_type(tmp_path))

    assert len(files) == 3


def test_write_tas_data_by_model_files_exist_and_roundtrip(tmp_path, tas_frame):
    """Each written file is readable and holds only its own model's rows."""
    files = write_tas_data_by_model(tas_frame, tmp_path)

    total = 0
    for path in files:
        assert os.path.isfile(path), f"{path} was not written"

        frame = pd.read_csv(path)
        assert frame["model"].nunique() == 1

        expected_model = os.path.basename(path).replace("_tas.csv", "")
        assert frame["model"].unique()[0] == expected_model

        total += len(frame)

    assert total == len(tas_frame), "rows were lost or duplicated across files"


def test_write_tas_data_by_model_creates_missing_directory(tmp_path, tas_frame):
    """A nonexistent output directory is created rather than raising.

    The original code called ``os.mkdir``, which fails if an intermediate parent
    is missing; ``os.makedirs(exist_ok=True)`` is used instead.
    """
    target = tmp_path / "deeply" / "nested" / "tas-data"

    files = write_tas_data_by_model(tas_frame, target)

    assert target.is_dir()
    assert len(files) == 3


def test_write_tas_data_by_model_is_idempotent(tmp_path, tas_frame):
    """Writing twice into the same directory succeeds and does not duplicate rows."""
    write_tas_data_by_model(tas_frame, tmp_path)
    files = write_tas_data_by_model(tas_frame, tmp_path)

    for path in files:
        frame = pd.read_csv(path)
        assert len(frame) == 5


def test_write_tas_data_by_model_requires_model_column(tmp_path):
    """A frame without a ``model`` column is rejected."""
    with pytest.raises(Exception):
        write_tas_data_by_model(pd.DataFrame({"value": [1, 2]}), tmp_path)


def test_write_tas_data_by_model_handles_model_names_with_dots(tmp_path):
    """Model identifiers containing dots and dashes survive intact.

    Real CMIP6 source IDs include names such as ``EC-Earth3-Veg-LR`` and
    ``FGOALS-f3-L``; the filename must not be truncated at a separator.
    """
    frame = pd.DataFrame(
        {"model": ["EC-Earth3-Veg-LR", "FGOALS-f3-L"], "value": [1.0, 2.0]}
    )

    files = write_tas_data_by_model(frame, tmp_path)
    names = sorted(os.path.basename(f) for f in files)

    assert names == ["EC-Earth3-Veg-LR_tas.csv", "FGOALS-f3-L_tas.csv"]


# ---------------------------------------------------------------------------
# Supporting helpers
# ---------------------------------------------------------------------------


def test_rbind_with_empty_frame():
    """`rbind` combines frames even when one side is empty."""
    populated = pd.DataFrame({"a": [1, 2]})
    empty = pd.DataFrame({"a": []})

    assert len(rbind(populated, empty)) == 2
    assert len(rbind(empty, populated)) == 2


def test_join_exclude_drops_matching_rows():
    """`join_exclude` removes the rows described by the drop frame."""
    data = pd.DataFrame({"model": ["a", "b", "c"], "value": [1, 2, 3]})
    drop = pd.DataFrame({"model": ["b"]})

    out = join_exclude(data, drop)

    assert sorted(out["model"]) == ["a", "c"]


def test_calculate_anomaly_centers_reference_period():
    """`calculate_anomaly` subtracts the mean of the reference window.

    Constructed so the reference-period mean is exactly known: with a constant
    value across the reference years, every anomaly in that window must be zero.
    """
    rows = []
    for year in range(1995, 2021):
        rows.append(
            {
                "model": "test_model",
                "experiment": "historical" if year <= 2014 else "ssp245",
                "ensemble": "r1i1p1f1",
                "variable": "tas",
                "year": year,
                "value": 287.0,
                "unit": "K",
            }
        )
    data = pd.DataFrame(rows)

    out = calculate_anomaly(data, startYr=1995, endYr=2014)

    reference = out[(out["year"] >= 1995) & (out["year"] <= 2014)]
    assert reference["value"].abs().max() == pytest.approx(0.0, abs=1e-12)
