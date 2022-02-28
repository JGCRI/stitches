# Define the functions that are used in the matching process.

import stitches.fx_util as util
import numpy as np
import pandas as pd


# Internal fx
def internal_dist(fx_pt, dx_pt, archivedata, tol=0):
    """ This function calculates the euclidean distance between the target values (fx and dx)
    and the archive values contained in the data frame. It will be used to help select which
    of the archive values best matches the target values. To ensure a consistent unit across
    all dimensions of the space, dx is updated to be windowsize*dx so that it has units of
    degC. This results in a distance metric (Euclidean/l2) in units of degC.
    Could _very_ easily make that choice of unit consistency optional via arg and if-statement.

        :param fx_pt:          a single value of the target fx value
        :param dx_pt:          a single value of the target dx value
        :param archivedata:    a data frame of the archive fx and dx values
        :param archivedata:    a data frame of the archive fx and dx values
        :param tol:            a tolerance for the neighborhood of matching. defaults to 0 degC - only the nearest-neighbor is returned

        :return:               a data frame with the target data and the corresponding matched archive data.
    """

    # Check the inputs
    util.check_columns(archivedata, {'model', 'experiment', 'variable',
                                     'ensemble', 'start_yr', 'end_yr', 'year', 'fx', 'dx'})

    # Compute the window size of the archive data to use to update
    # dx values to be windowsize*dx so that it has units of degC
    windowsize = max(archivedata['end_yr'] - archivedata['start_yr'])

    # TODO ACS do we need to implement this check?
    # # For now comment out the code that enforces that there is only type of window size,
    # # however we do know that when the full ts is not divisible by a window size of 9
    # # or what not then we will run into issues with this. We do know that is will be
    # # important to ennsure that the size of the target chunk and archive chunks are equivalent.
    # TODO address this issue at a latter time
    # if(length(windowsize) > 1){
    #  stop("you've made your archive by chunking with multiple window sizes, don't do that!")
    # }

    # Update the names of the columns in the archive data.
    dist = archivedata.copy()
    dist.columns = 'archive_' + dist.columns

    # calculate the euclidean distance between the target point
    # and the archive observations.
    # Calculating distance d(target, archive_i) for each point i in the archive.
    # calculating the distance in the dx and fx dimensions separately because
    # now we want to track those in addition to the l2 distance.
    dist["dist_dx"] = windowsize * abs(dist["archive_dx"] - dx_pt)
    dist["dist_fx"] = abs(dist['archive_fx'] - fx_pt)git
    dist["dist_l2"] = (dist["dist_fx"] ** 2 + dist["dist_dx"] ** 2) ** .5

    # this returns the first minimum run into, which is not how we are going to want to do it,
    # we will want some way to keep track of the min and combine to have different realizations
    # or have some random generation. But for now I think that this is probably sufficent.
    min_dist = dist['dist_l2'][np.argmin(dist['dist_l2'])]
    dist_radius = min_dist + tol
    index = np.where(dist["dist_l2"].values <= dist_radius)
    out = dist.loc[index]

    out["target_fx"] = fx_pt
    out["target_dx"] = dx_pt

    return out

# Internal fx
def shuffle_function(dt):
    """ Randomly shuffle the deck, this should help with the matching process.

        :param dt:          a data of archive values that will be used in the matching process.

        :return:               a randomly ordered data frame.
    """
    nrow = dt.shape[0]
    out = dt.sample(nrow, replace=False)
    out = out.reset_index(drop=True)
    return out

# Internal fx
def drop_hist_false_duplicates(matched_data):
    """ A helper function to remove false duplicate matches in the historical period. For
    example, target 1850 gets 1872 data from realization 13 of SSP126 and SSP585.
    The metadata of these archive values are different, but the actual data
    values are identical because we just pasted in the same historical data to
    every Experiment. So this function keeps only the first match.

        :param matched_data:    pandas object returned from match_neighborhood.

        :return:               a data frame of matched data with the same structure as the input, with false duplicates in the historical period droppe
    """

    # TODO ACS this fx had to be modified to account for the idealized exps, please review carefuly.
    # Subset the idealized runs, since these are not concatenated with the historical time series
    # they can be left alone.
    idealized_exps = ['1pctCO2', 'abrupt-4xCO2', 'abrupt-2xCO2']
    idealized_matched_data = matched_data.loc[matched_data['archive_experiment'].isin(idealized_exps)].copy()

    # Now select the non idealized runs, these are the time series that are a combination of the
    # historical and future time series.
    fut_matched_data = matched_data.loc[~matched_data['archive_experiment'].isin(idealized_exps)].copy()

    # Determine the historical cut off year based on the size of the chunks.
    cut_off_yr = 2015 - max(fut_matched_data["target_end_yr"] - fut_matched_data["target_start_yr"]) / 2

    # Because smoothing with window =9 has occured,
    # historical is actually 2010 or earlier: the chunks
    # that had purely historical data in them and none
    # from the future when smoothing.
    subset_df = fut_matched_data.loc[fut_matched_data["target_year"] <= cut_off_yr].copy()

    # Only operate if there are any historic years to deal with:
    if (util.nrow(subset_df) > 0):
        # parse out information about the ssp experiment id
        subset_df["idvalue"] = list(
            map(lambda x: int(x.split("p")[1].replace('-over', '')), subset_df["archive_experiment"]))
        # group the data frame by the target information, recall that for each target match
        # there might be more matches with the archive. Here we want to make sure that we do
        # not want to have multiple matches with experiments from the historical period
        # from the archive.
        grouped_dat = subset_df.groupby(["target_variable", "target_experiment", "target_ensemble",
                                         "target_model", "target_start_yr", "target_end_yr", "target_year",
                                         "target_fx", "target_dx", "archive_ensemble", "archive_year"])
        dat = []
        for name, group in grouped_dat:
            min_id_value = min(group['idvalue'])
            dat.append(group.loc[group['idvalue'] == min_id_value])
        historical = pd.concat(dat)
        cols_to_keep = ['target_variable', 'target_experiment', 'target_ensemble',
                        'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                        'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                        'archive_model', 'archive_ensemble', 'archive_start_yr',
                        'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                        'dist_fx', 'dist_l2']
        historical = historical[cols_to_keep]

        # select the future matched data
        # TODO should it be 2010 or should it be the cut off year?
        matched = pd.concat([fut_matched_data.loc[fut_matched_data['target_year'] > 2010].copy(),
                             historical, idealized_matched_data])
        matched = matched.sort_values('target_year')
        matched = matched.reset_index()
        cols_to_keep = ['target_variable', 'target_experiment', 'target_ensemble',
                        'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                        'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                        'archive_model', 'archive_ensemble', 'archive_start_yr',
                        'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                        'dist_fx', 'dist_l2']
        matched = matched[cols_to_keep]
        return matched
    else:
        return matched_data

def match_neighborhood(target_data, archive_data, tol=0, drop_hist_duplicates=True):
    # Check the inputs of the functions
    if util.nrow(target_data) <= 0:
        raise TypeError(f"target_data is an empty data frame")
    if util.nrow(archive_data) <= 0:
        raise TypeError(f"archive_data is an empty data frame")
    util.check_columns(archive_data, {"experiment", "variable", "ensemble", "start_yr", "end_yr", "fx", "dx"})
    util.check_columns(target_data, {"start_yr", "end_yr", "fx", "dx"})

    # shufflle the the archive data
    archive_data = shuffle_function(archive_data)

    # For every entry in the target data frame find its nearest neighboor from the archive data.
    # concatenate the results into a single data frame.
    rslt = map(lambda fx, dx: internal_dist(fx, dx, archivedata=archive_data, tol=tol),
               target_data["fx"], target_data["dx"])
    matched = pd.concat(list(rslt))

    # Now add the information about the matches to the target data
    # Make sure it if clear which columns contain  data that comes from the target compared
    # to which ones correspond to the archive information. Right now there are lots of columns
    # that contain duplicate information for now it is probably fine to be moving these things around.
    #
    # Make a copy of the target data to work with and use in creating the output matched data frame,
    # so that this function doesn't change the column names of one of its arguments.
    target_data_copied = target_data.copy()
    target_data_copied.columns = 'target_' + target_data_copied.columns

    # Now add the information about the matches to the target data
    # Make sure it if clear which columns contain  data that comes from the target compared
    # to which ones correspond to the archive information. Right now there are lots of columns
    # that contain duplicate information for now it is probably fine to be moving these things around.
    out = matched.merge(target_data_copied, how='left', on=['target_fx', 'target_dx'])
    cols_to_keep = ["target_variable", "target_experiment", "target_ensemble", "target_model",
                    "target_start_yr", "target_end_yr", "target_year", "target_fx", "target_dx",
                    "archive_experiment", "archive_variable", "archive_model", "archive_ensemble",
                    "archive_start_yr", "archive_end_yr", "archive_year", "archive_fx", "archive_dx",
                    "dist_dx", "dist_fx", "dist_l2"]
    out = out[cols_to_keep].drop_duplicates()

    if drop_hist_duplicates:
        out = drop_hist_false_duplicates(out)

    return out


