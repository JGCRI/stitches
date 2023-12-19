# Save a copy of the pangeo table, this has information about the z store file.
from importlib import resources

import pandas as pd

import stitches.fx_pangeo as pangeo


def make_pangeo_table():
    """
    Create a copy of the Pangeo files that have corresponding entries in the matching archive.

    This function is used in the stitching process to ensure that only relevant Pangeo files
    are considered. It writes out a file to the package data directory.

    :return: None
    """
    # Using the information about what experiment/ensemble/models that are available for matching.
    archive_path = resources.files("stitches") / "data" / "matching_archive.csv"
    tas_exp_model = (
        pd.read_csv(archive_path)[["experiment", "ensemble", "model"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # For the future experiments make sure we add in the historical experiment info.
    fut_exps = ["ssp126", "ssp245", "ssp370", "ssp585", "ssp534-over"]
    hist_info = tas_exp_model.loc[tas_exp_model["experiment"].isin(fut_exps)].copy()[
        ["model", "ensemble"]
    ]
    hist_info["experiment"] = "historical"
    hist_info = hist_info.drop_duplicates()
    to_keep = pd.concat([tas_exp_model, hist_info], axis=0)
    to_keep = to_keep.reset_index(drop=True)

    # Get the complete data table from pangeo
    dat = pangeo.fetch_pangeo_table()
    pangeo_table = dat.loc[dat["grid_label"] == "gn"][
        ["source_id", "experiment_id", "member_id", "variable_id", "zstore", "table_id"]
    ].copy()
    pangeo_table = pangeo_table.rename(
        columns={
            "source_id": "model",
            "experiment_id": "experiment",
            "member_id": "ensemble",
            "variable_id": "variable",
            "zstore": "zstore",
            "table_id": "domain",
        }
    )
    pangeo_table = pangeo_table.reset_index(drop=True)

    # Subset the pangeo table so that it only contains information for what we have deemed as good data.
    final_pangeo_table = pangeo_table.merge(
        to_keep, how="inner", on=["model", "experiment", "ensemble"]
    )

    # Write the file out
    out_file = resources.files("stitches") / "data" / "pangeo_table.csv"
    final_pangeo_table.to_csv(out_file, index=False)

    # Return
    return None


def make_pangeo_comparison():
    """
    Create a copy of the entire Pangeo archive for testing.

    This function is used to check for updates in the Pangeo archive. If an update is
    detected, it may suggest updating the internal package data.

    :return: None. Writes a file to package data.
    """

    dat = pangeo.fetch_pangeo_table()
    out_file = resources.files("stitches") / "data" / "pangeo_comparison_table.csv"
    dat.to_csv(out_file, index=False)
    # Return
    return None
