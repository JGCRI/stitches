"""
The `fx_match` module is responsible for matching target climate model outputs with archived model outputs.

It includes functions to calculate distances between target and archive data points and to
select the best matches based on those distances.
"""

import numpy as np
import pandas as pd

import stitches.fx_util as util


# Internal fx
def internal_dist(fx_pt, dx_pt, archivedata, tol=0):
    """
    Calculate the Euclidean distance between target and archive values.

    This function calculates the Euclidean distance between the target values (fx and dx)
    and the archive values contained in the dataframe. It is used to select which
    archive values best match the target values. To ensure consistent units across
    all dimensions, dx is updated to be windowsize*dx with units of degC, resulting
    in a distance metric (Euclidean/l2) in units of degC. The choice of unit consistency
    could be made optional via an argument and if-statement.

    :param fx_pt: A single value of the target fx value.
    :param dx_pt: A single value of the target dx value.
    :param archivedata: A dataframe of the archive fx and dx values.
    :param tol: A tolerance for the neighborhood of matching; defaults to 0 degC,
                returning only the nearest neighbor.
    :return: A dataframe with the target data and the corresponding matched archive data.
    """
    # Check the inputs
    util.check_columns(
        archivedata,
        {
            "model",
            "experiment",
            "variable",
            "ensemble",
            "start_yr",
            "end_yr",
            "year",
            "fx",
            "dx",
        },
    )

    # Compute the window size of the archive data to use to update
    # dx values to be windowsize*dx so that it has units of degC
    windowsize = max(archivedata["end_yr"] - archivedata["start_yr"])

    # Update the names of the columns in the archive data.
    dist = archivedata.copy()
    dist.columns = "archive_" + dist.columns

    # calculate the euclidean distance between the target point
    # and the archive observations.
    # Calculating distance d(target, archive_i) for each point i in the archive.
    # calculating the distance in the dx and fx dimensions separately because
    # now we want to track those in addition to the l2 distance.
    dist["dist_dx"] = windowsize * abs(dist["archive_dx"] - dx_pt)
    dist["dist_fx"] = abs(dist["archive_fx"] - fx_pt)
    dist["dist_l2"] = (dist["dist_fx"] ** 2 + dist["dist_dx"] ** 2) ** 0.5

    # this returns the first minimum run into, which is not how we are going to want to do it,
    # we will want some way to keep track of the min and combine to have different realizations
    # or have some random generation. But for now I think that this is probably sufficient.
    min_dist = dist["dist_l2"][np.argmin(dist["dist_l2"])]
    dist_radius = min_dist + tol
    index = np.where(dist["dist_l2"].values <= dist_radius)
    out = dist.loc[index]

    out["target_fx"] = fx_pt
    out["target_dx"] = dx_pt

    return out


# Internal fx
def shuffle_function(dt):
    """
    Randomly shuffle the deck to assist with the matching process.

    :param dt: A DataFrame of archive values used in the matching process.
    :return: A DataFrame with rows in random order.
    """
    nrow = dt.shape[0]
    out = dt.sample(nrow, replace=False)
    out = out.reset_index(drop=True)
    return out


# Internal fx
def drop_hist_false_duplicates(matched_data):
    """
    Remove false duplicate matches in the historical period.

    This function is used to remove false duplicate matches in the historical period.
    For example, if the target year 1850 gets data from 1872 from realization 13 of
    SSP126 and SSP585, the metadata of these archive values are different, but the
    actual data values are identical because the same historical data was pasted into
    every experiment. This function keeps only the first match.

    :param matched_data: pandas DataFrame returned from match_neighborhood.
    :return: DataFrame with the same structure as the input, with false duplicates
             in the historical period dropped.
    """
    # Subset the idealized runs, since these are not concatenated with the historical time series
    # they can be left alone.
    idealized_exps = ["1pctCO2", "abrupt-4xCO2", "abrupt-2xCO2"]
    idealized_matched_data = matched_data.loc[
        matched_data["archive_experiment"].isin(idealized_exps)
    ].copy()

    # Now select the non idealized runs, these are the time series that are a combination of the
    # historical and future time series.
    fut_matched_data = matched_data.loc[
        ~matched_data["archive_experiment"].isin(idealized_exps)
    ].copy()

    # Determine the historical cut off year based on the size of the chunks.
    cut_off_yr = (
        2015
        - max(fut_matched_data["target_end_yr"] - fut_matched_data["target_start_yr"])
        / 2
    )

    # Because smoothing with window =9 has occurred,
    # historical is actually 2010 or earlier: the chunks
    # that had purely historical data in them and none
    # from the future when smoothing.
    subset_df = fut_matched_data.loc[
        fut_matched_data["target_year"] <= cut_off_yr
    ].copy()

    # Only operate if there are any historic years to deal with:
    if util.nrow(subset_df) > 0:
        # parse out information about the ssp experiment id
        subset_df["idvalue"] = list(
            map(
                lambda x: int(x.split("p")[1].replace("-over", "")),
                subset_df["archive_experiment"],
            )
        )
        # group the data frame by the target information, recall that for each target match
        # there might be more matches with the archive. Here we want to make sure that we do
        # not want to have multiple matches with experiments from the historical period
        # from the archive.
        grouped_dat = subset_df.groupby(
            [
                "target_variable",
                "target_experiment",
                "target_ensemble",
                "target_model",
                "target_start_yr",
                "target_end_yr",
                "target_year",
                "target_fx",
                "target_dx",
                "archive_ensemble",
                "archive_year",
            ]
        )
        dat = []
        for name, group in grouped_dat:
            min_id_value = min(group["idvalue"])
            dat.append(group.loc[group["idvalue"] == min_id_value])
        historical = pd.concat(dat)
        cols_to_keep = [
            "target_variable",
            "target_experiment",
            "target_ensemble",
            "target_model",
            "target_start_yr",
            "target_end_yr",
            "target_year",
            "target_fx",
            "target_dx",
            "archive_experiment",
            "archive_variable",
            "archive_model",
            "archive_ensemble",
            "archive_start_yr",
            "archive_end_yr",
            "archive_year",
            "archive_fx",
            "archive_dx",
            "dist_dx",
            "dist_fx",
            "dist_l2",
        ]
        historical = historical[cols_to_keep]

        # select the future matched data
        # TODO should it be 2010 or should it be the cut off year?
        matched = pd.concat(
            [
                fut_matched_data.loc[fut_matched_data["target_year"] > 2010].copy(),
                historical,
                idealized_matched_data,
            ]
        )
        matched = matched.sort_values("target_year")
        matched = matched.reset_index()
        cols_to_keep = [
            "target_variable",
            "target_experiment",
            "target_ensemble",
            "target_model",
            "target_start_yr",
            "target_end_yr",
            "target_year",
            "target_fx",
            "target_dx",
            "archive_experiment",
            "archive_variable",
            "archive_model",
            "archive_ensemble",
            "archive_start_yr",
            "archive_end_yr",
            "archive_year",
            "archive_fx",
            "archive_dx",
            "dist_dx",
            "dist_fx",
            "dist_l2",
        ]
        matched = matched[cols_to_keep]
        return matched
    else:
        return matched_data


def match_neighborhood(
    target_data, archive_data, tol: float = 0, drop_hist_duplicates: bool = True
):
    """
    Calculate the Euclidean distance between target and archive data.

    This function takes data frames of target and archive data and calculates the
    Euclidean distance between the target values (fx and dx) and the archive values.

    :param target_data: Data frame of the target fx and dx values.
    :param archive_data: Data frame of the archive fx and dx values.
    :param tol: Tolerance for the neighborhood of matching. Defaults to 0 degC,
        meaning only the nearest-neighbor is returned. Must be a float.
    :param drop_hist_duplicates: Determines whether to consider historical values
        across SSP scenarios as duplicates (True) and drop all but one from matching,
        or to consider them as distinct points for matching (False). Defaults to True.
    :type drop_hist_duplicates: bool
    :return: Data frame with the target data and the corresponding matched archive data.
    """
    # Check the inputs of the functions
    if util.nrow(target_data) <= 0:
        raise TypeError("target_data is an empty data frame")
    if util.nrow(archive_data) <= 0:
        raise TypeError("archive_data is an empty data frame")
    util.check_columns(
        archive_data,
        {"experiment", "variable", "ensemble", "start_yr", "end_yr", "fx", "dx"},
    )
    util.check_columns(target_data, {"start_yr", "end_yr", "fx", "dx"})

    archive_data = archive_data.reset_index(drop=True).copy()

    # For every entry in the target data frame find its nearest neighbor from the archive data.
    # concatenate the results into a single data frame.
    rslt = map(
        lambda fx, dx: internal_dist(fx, dx, archivedata=archive_data, tol=tol),
        target_data["fx"],
        target_data["dx"],
    )
    matched = pd.concat(list(rslt))

    # Now add the information about the matches to the target data
    # Make sure it is clear which columns contain  data that comes from the target compared
    # to which ones correspond to the archive information.
    #
    # Make a copy of the target data to work with and use in creating the output matched data frame,
    # so that this function doesn't change the column names of one of its arguments.
    target_data_copied = target_data.copy()
    target_data_copied.columns = "target_" + target_data_copied.columns

    out = matched.merge(target_data_copied, how="left", on=["target_fx", "target_dx"])
    cols_to_keep = [
        "target_variable",
        "target_experiment",
        "target_ensemble",
        "target_model",
        "target_start_yr",
        "target_end_yr",
        "target_year",
        "target_fx",
        "target_dx",
        "archive_experiment",
        "archive_variable",
        "archive_model",
        "archive_ensemble",
        "archive_start_yr",
        "archive_end_yr",
        "archive_year",
        "archive_fx",
        "archive_dx",
        "dist_dx",
        "dist_fx",
        "dist_l2",
    ]
    out = out[cols_to_keep].drop_duplicates()

    if drop_hist_duplicates:
        out = drop_hist_false_duplicates(out)

    # if there are any nearest neighbor matches that are maybe still large,
    # warn the user that they will want to validate the outcome.

    # get the nearest neighbor match only for each target window
    grouped = out.groupby(
        [
            "target_variable",
            "target_experiment",
            "target_ensemble",
            "target_model",
            "target_start_yr",
            "target_end_yr",
            "target_year",
            "target_fx",
            "target_dx",
        ]
    )
    formatted_nn = pd.DataFrame()
    for name, group in grouped:
        df = group.copy()
        df = df[df["dist_l2"] == np.min(df["dist_l2"])].copy()
        formatted_nn = pd.concat([formatted_nn, df]).reset_index(drop=True)
        del df

    # subset to just the far away nearest neighbors
    formatted_nn = (
        formatted_nn[formatted_nn["dist_l2"] > 0.25].reset_index(drop=True).copy()
    )

    if not formatted_nn.empty:
        print("The following target windows have a nearest neighbor in T, dT space")
        print("that is more than 0.25degC away. This may or may not result in poor")
        print("matches and we recommend validation.")
        print(formatted_nn)
        print(
            "-----------------------------------------------------------------------------------------"
        )
    del formatted_nn
    del grouped

    return out
