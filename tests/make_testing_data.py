from importlib import resources

import pandas as pd
import pytest

import stitches
from stitches.fx_match import match_neighborhood


@pytest.fixture(scope="session", autouse=True)
def setup_package_data():
    """
    Set up the package data for testing.

    This fixture is automatically used in tests that require the package data.
    It installs the package data and prepares the testing environment.
    """

    stitches.install_package_data()

    archive_path = resources.files("stitches") / "data" / "matching_archive.csv"
    data = pd.read_csv(archive_path)

    target_data = data[data["model"] == "BCC-CSM2-MR"]
    target_data = target_data[target_data["experiment"] == "ssp245"]
    target_data = target_data[target_data["ensemble"] == "r1i1p1f1"]
    target_data = target_data.reset_index(drop=True)

    archive_data = data[data["model"] == "BCC-CSM2-MR"].copy()
    match_df = match_neighborhood(
        target_data, archive_data, tol=0.1, drop_hist_duplicates=False
    )
    match_path = resources.files("stitches") / "tests" / "test-match_w_dup.csv"
    match_df.to_csv(match_path, index=False)

    match_df = match_neighborhood(
        target_data, archive_data, tol=0.1, drop_hist_duplicates=True
    )
    match_path = resources.files("stitches") / "tests" / "ttest-match.csv"
    match_df.to_csv(match_path, index=False)

    path = resources.files("stitches") / "tests" / "test-target_dat.csv"
    tar = pd.read_csv(path)

    path = resources.files("stitches") / "tests" / "test-archive_dat.csv"
    arch = pd.read_csv(path)

    match_df = match_neighborhood(tar, arch, tol=0.1, drop_hist_duplicates=True)
    match_path = resources.files("stitches") / "tests" / "test-match_dat.csv"
    match_df.to_csv(match_path, index=False)
