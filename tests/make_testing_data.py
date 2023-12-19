# This script generates the csv files that are used as comparison data
# in the stitches unit testing. It also seems like some of the testing
# data wasn't actually included in this repo
# TODO this script needs to be revisited
from importlib import resources

import pandas as pd

from stitches.fx_match import match_neighborhood

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
