"""Fixtures and configuration for the performance benchmarks.

Provides synthetic data at several scales so benchmarks can show how each
function grows with input size, not just how long it takes on one fixed input.
Scaling behavior is what identifies the functions worth optimizing: a routine
whose cost grows quadratically with the number of target windows is a much better
target than one that is merely slow but linear.
"""

from importlib import resources
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Every benchmark in this directory carries the `benchmark` marker.
pytestmark = pytest.mark.benchmark


def pytest_configure(config):
    """Register the markers this directory uses.

    Declared here as well as in ``pytest.ini`` so that ``pytest benchmarks`` works
    when invoked directly from this directory, where the root config may not be
    picked up.
    """
    for marker in (
        "benchmark: performance measurement, not a correctness assertion",
        "slow: long-running benchmark (>30s)",
        "network: requires internet access",
        "package_data: requires the full Zenodo-minted package data",
    ):
        config.addinivalue_line("markers", marker)


def make_series(n_years=251, model="test_model", experiment="ssp245", ensemble="r1i1p1f1"):
    """Build a deterministic synthetic annual temperature series.

    :param n_years: Number of annual values to generate.
    :return: A DataFrame with the columns the processing functions require.
    :rtype: pandas.DataFrame
    """
    years = np.arange(1850, 1850 + n_years)
    rng = np.random.default_rng(20240101)

    return pd.DataFrame(
        {
            "year": years,
            "value": 0.00012 * (years - 1850) ** 2 + rng.normal(0, 0.05, size=n_years),
            "variable": "tas",
            "model": model,
            "experiment": experiment,
            "ensemble": ensemble,
            "unit": "K",
        }
    )


def make_chunk_frame(n_windows, ensemble="r1i1p1f1", experiment="ssp245", offset=0.0):
    """Build a synthetic chunked archive with ``n_windows`` time windows.

    Mirrors the shape produced by ``get_chunk_info``: one row per 9-year window
    carrying the level (``fx``) and rate of change (``dx``).

    :param n_windows: Number of windows to generate.
    :param offset: Constant added to ``fx``, used to make archive members differ.
    :return: A DataFrame shaped like a matching archive.
    :rtype: pandas.DataFrame
    """
    starts = 1850 + 9 * np.arange(n_windows)
    rng = np.random.default_rng(abs(hash((ensemble, experiment))) % (2**32))

    return pd.DataFrame(
        {
            "experiment": experiment,
            "variable": "tas",
            "ensemble": ensemble,
            "model": "test_model",
            "start_yr": starts,
            "end_yr": starts + 8,
            "year": starts + 4,
            "fx": np.linspace(-1.3, 3.0, n_windows) + offset,
            "dx": rng.normal(0.02, 0.01, size=n_windows),
        }
    )


def make_archive(n_windows, n_members=4):
    """Build a synthetic archive spanning several ensemble members.

    :param n_windows: Windows per member.
    :param n_members: Number of ensemble members.
    :return: A concatenated archive DataFrame.
    :rtype: pandas.DataFrame
    """
    frames = [
        make_chunk_frame(
            n_windows, ensemble=f"r{i + 1}i1p1f1", offset=0.01 * i
        )
        for i in range(n_members)
    ]
    return pd.concat(frames).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def example_dir():
    """Return the directory of committed example CSVs."""
    return Path(str(resources.files("stitches") / "data" / "example"))


@pytest.fixture(scope="session")
def example_target(example_dir):
    """Return the committed example target data."""
    return pd.read_csv(example_dir / "test-target_dat.csv")


@pytest.fixture(scope="session")
def example_archive(example_dir):
    """Return the committed example archive data."""
    return pd.read_csv(example_dir / "test-archive_dat.csv")


@pytest.fixture(scope="session")
def example_match_with_duplicates(example_dir):
    """Return committed match data that still contains historical duplicates."""
    return pd.read_csv(example_dir / "test-match_w_dup.csv")


@pytest.fixture(scope="session")
def long_series():
    """Return a single 251-year series, the full CMIP6 1850-2100 span."""
    return make_series()


@pytest.fixture(scope="session")
def multi_member_series():
    """Return a multi-experiment, multi-member series for grouped operations."""
    frames = [
        make_series(experiment=exp, ensemble=ens)
        for exp in ("historical", "ssp126", "ssp245", "ssp585")
        for ens in (f"r{i}i1p1f1" for i in range(1, 6))
    ]
    return pd.concat(frames).reset_index(drop=True)
