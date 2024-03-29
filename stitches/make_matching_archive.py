"""
This module defines functions used to create the archive for the matching process.

It calculates the rate of change (dx) and median value (fx) for the temperature anomaly time series.
"""

from importlib import resources

import numpy as np
import pandas as pd

# Import packages
import stitches.fx_processing as prep
import stitches.fx_util as util


def make_matching_archive(
    smoothing_window: int = 9, chunk_window: int = 9, add_staggered: bool = False
):
    """
    Create an archive of rate of change (dx) and mean (fx) values.

    This function processes the CMIP6 archive to produce values used in the
    matching portion of the stitching pipeline.

    :param smoothing_window: The size of the smoothing window to be applied to the time series.
                             Defaults to 9.
    :type smoothing_window: int

    :param chunk_window: The size of the chunks of data to summarize with dx & fx.
                         Defaults to 9.
    :type chunk_window: int

    :param add_staggered: If True, staggered windows will be added to the archive.
                          Defaults to False.
    :type add_staggered: bool

    :return: The file location of the matching archive.
    """
    # Start by loading all of the tas files.
    raw_data = util.load_data_files("data/tas-data")
    raw_data = raw_data[raw_data["year"] <= 2100].copy()

    # Smooth the anomalies, get the running mean of each time series.
    # Each year in the original time series is retained, and the running mean
    # recorded for each year is the mean centered on that year across
    # smoothing_window number of years.
    smoothed_data = prep.calculate_rolling_mean(raw_data, smoothing_window)

    # For each group in the data set go through, chunk and extract the fx and dx␣ values.
    # For now we have to do this with a for loop, to process each model/experiment/ensemble/variable
    # individually.
    # The key function prepfor aring these chunks is prep.chunk_ts
    data = smoothed_data[
        ["model", "experiment", "ensemble", "year", "variable", "value"]
    ]
    data = data.reset_index(drop=True).copy()
    group_by = ["model", "experiment", "ensemble", "variable", "unit"]
    out = []
    for key, d in data.groupby(group_by):
        dat = d.reset_index(drop=True).copy()

        # if this data set doesn't have at least chunk_window worth of years,
        # just print a message that it isn't getting processed into chunks.
        # (it doesn't make sense to create a 9 year chunk window from 6
        # years of data, and it causes issues when we want to add the staggered
        if dat["year"].nunique() < chunk_window:
            mod = dat.model.unique()[0]
            exp = dat.experiment.unique()[0]
            ens = dat.ensemble.unique()[0]
            print(
                mod
                + "  "
                + exp
                + "  "
                + ens
                + "  has fewer than chunk_window="
                + str(chunk_window)
                + " years in its time series. Skipping"
            )
        else:
            dd = prep.chunk_ts(df=dat, n=chunk_window)
            rslt = prep.get_chunk_info(dd)
            rslt["unit"] = (np.repeat(d["unit"].unique(), len(rslt))).copy()
            out.append(rslt)
        # end if-else
    # end of the for loop

    # if adding staggered windows, do it now.
    if add_staggered:
        # for each offset, do the prep and append.
        for offset in range(1, chunk_window):
            for key, d in data.groupby(group_by):
                dat = d.reset_index(drop=True).copy()

                # if this data set doesn't have at least chunk_window worth of years,
                # just print a message that it isn't getting processed into chunks.
                if dat["year"].nunique() < chunk_window:
                    mod = dat.model.unique()[0]
                    exp = dat.experiment.unique()[0]
                    ens = dat.ensemble.unique()[0]
                    print(
                        mod
                        + "  "
                        + exp
                        + "  "
                        + ens
                        + "  has fewer than chunk_window="
                        + str(chunk_window)
                        + " years in its time series. Skipping"
                    )
                else:
                    dd = prep.chunk_ts(df=dat, n=chunk_window, base_chunk=offset)
                    rslt = prep.get_chunk_info(dd)
                    rslt["unit"] = (np.repeat(d["unit"].unique(), len(rslt))).copy()
                    out.append(rslt)
                # end if-else
            # end of the for loop over (model-experiment-ensemble-variable) combos
        # end for loop over base_chunk offsets
    # end if statement for adding staggered chunks

    #  concatenate results into a single data frame.
    data = pd.concat(out).reset_index(drop=True)

    outdir_path = resources.files("stitches") / "data"
    if add_staggered:
        ofile = outdir_path + "/matching_archive_staggered.csv"
        # for the staggered archive, because we've added so many points to
        # the archive, we keep only the points based on chunk_window years of
        # data (basically cutting out head and tail points, which we can afford
        # to lose).
        # This way, we don't have to change any of our stitching functions to handle
        # those cases when we use the staggered archive
        data = data[(data["end_yr"] - data["start_yr"] + 1 == chunk_window)].copy()
        data.to_csv(ofile, index=False)
    else:
        ofile = outdir_path + "/matching_archive.csv"
        data.to_csv(ofile, index=False)

    return ofile
