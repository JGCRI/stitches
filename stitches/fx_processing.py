"""Module for processing raw tas time series into 'chunks' of data used in the matching process."""

import math as math

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

import stitches.fx_util as util


def calculate_rolling_mean(data, size):
    """
    Calculate the rolling mean for the data frame with a user-defined size centered window.

    :param data: A data frame of the CMIP absolute temperature.
    :type data: pandas.core.frame.DataFrame
    :param size: An integer value for the size of the window to use when calculating the rolling mean.
    :type size: int
    :return: A pandas data frame of the smoothed time series with the rolling mean applied.
    """
    # Check inputs
    util.check_columns(
        data, {"model", "experiment", "ensemble", "year", "value", "variable"}
    )

    # Index the data frame by the year, so that the rolling mean respects the years
    data.index = data["year"]

    group_by = ["model", "experiment", "ensemble", "variable"]

    # previously, we just had a call:
    # rslt = data.groupby(group_by)['value'].rolling(size, center=True).mean().reset_index()
    # This returns the proper rolling mean on grouped data, but the first and last (size-1)/2
    # values in each grouped time series return as NaN because the full window isn't available
    # on both sides of the values there. We want to still have 'semi-smoothed' values there, as
    # in, we average whatever data we have in each grouped time series. So for a window size=5,
    # the smoothed 2099 value would be the average of 2097-2100 in our goal set up (as opposed
    # to 2097-2101 if we had the full 5 years of data centered on 2099).
    # In theory, pd.rolling(min_periods=1) does exactly what we want; however, it's weird on
    # grouped data. Sticking with the example of window size = 5, it was bringing in values from the
    # next group's time series to fill in 2099 and 2100.
    # To get pandas.rolling(min_periods-1) to apply correctly to grouped data, we have to wrap it
    # in a call to `transform` and save that as a new column:

    rslt = data.copy()
    rslt["rollingAvg"] = rslt.groupby(group_by)["value"].transform(
        lambda x: x.rolling(size, center=True, min_periods=1).mean()
    )
    # ^ seems like the way people usually get the functionality of a
    # dplyr group_by %>% mutate()  call.

    # rename so that value is the smoothed data:
    rslt = (
        rslt.drop(columns="value", axis=1)
        .rename(columns={"rollingAvg": "value"})
        .reset_index(drop=True)
    )

    return rslt


def chunk_ts(df, n, base_chunk=0):
    """
    Format a data frame into an array of data frames with n-sized years of successive data.

    This function takes a data frame of climate data and chunks it into separate periods,
    each containing data for a span of `n` years. It adds a 'chunk' column to the data frame
    to indicate the period each row belongs to.

    :param df: Data frame of climate data to chunk into different periods.
    :type df: pandas.DataFrame
    :param n: The size of the windows to chunk into separate periods.
    :type n: int
    :param base_chunk: A helper argument for creating staggered chunks, defaults to 0 (original behavior).
    :type base_chunk: int
    :return: A pandas DataFrame identical to `df` with the addition of a 'chunk' column.
    """
    # Check inputs
    df = df.drop_duplicates().reset_index(drop=True).copy()
    util.check_columns(df, {"year", "variable", "value"})
    if not (len(df["variable"].unique()) == 1):
        raise TypeError(f'Multiple variables discovered in "{df}"')

    # do the offset for the staggered windows
    if base_chunk > n:
        raise TypeError("base_chunk cannot be larger than n")
    df = df[base_chunk:].copy()

    # Add a column of data that categorizes the data into the different periods, right now we allow for
    # a list that is smaller than n at the end.
    # Save a copy of the length of the list
    yr_len = len(df["year"].unique())

    # Determine the number of unique n sized chunks, right now we assume that at most a chunk
    # can have n entries or less.
    n_chunks = math.ceil(yr_len / n)
    # Make an np array of the chunk labels, where each label repeats n times.
    chunk_labels = np.repeat(list(range(0, n_chunks)), n)
    # Subset the chunk list so that matches the length of the data frame.
    chunk_labels = chunk_labels[range(0, yr_len)]

    df["chunk"] = chunk_labels

    return df


def get_chunk_info(df):
    """
    Determine the value and the rate of change for each chunk.

    :param df: Data frame of climate data chunked into different periods.
    :type df: pandas.DataFrame
    :return: A pandas DataFrame with the chunk information, including the start and end years, the chunk value (fx),
             and the chunk rate of change (dx).
    """
    # Check the inputs
    flag = "index" in df.columns
    if flag:
        del df["index"]

    util.check_columns(df, {"year", "variable", "value", "chunk"})
    if not (len(df["variable"].unique()) == 1):
        raise TypeError(f'Multiple variables discovered in "{df}"')

    # Save information that will be added to
    extra_columns = list(set(df.columns).difference({"chunk", "value", "year"}))
    extra_info = df[extra_columns].drop_duplicates()
    if not (len(extra_info) == 1):
        raise TypeError("More than one type of data being read into the function.")

    # Group the data frame by the chunk label so that we can use a for loop
    # to extract information from each chunk of data.
    df_gby = df.groupby("chunk")

    # Use the for loop to work our way through the different chunks/periods of
    # data, gathering information about each start and stop year, the central
    # value, and the rate of change.
    index = 0
    for key, chunk in df_gby:
        # Save some information about the period of data, when it starts and stops.
        chunk["year"] = chunk["year"].astype(int)
        start_yr = min(chunk["year"])
        end_yr = max(chunk["year"])

        # Get the fx, the value of the center of the time period
        x = math.ceil(
            np.median(chunk["year"])
        )  # right now we select the one from the high year
        fx = chunk[chunk["year"] == x]["value"].values[0]

        # Prepare the x and y inputs from the data frame for the linear regression.
        # LinearRegression imported from sklearn.linear_model requires numpy arrays
        # instead of pandas objects.
        x_input = chunk["year"].values.reshape(-1, 1)
        y_input = chunk["value"].values.reshape(-1, 1)

        # Fit a linear regression of the period of time, extract the slope or dx
        # from the fit. Make sure that the dx is floating value that can be easily
        # stored in a data frame.
        model = LinearRegression()
        model.fit(x_input, y_input)
        dx = float(model.coef_[0])

        # Format the the chunk data into a pandas data frame.
        row = pd.DataFrame(
            [[start_yr, end_yr, x, fx, dx]],
            columns=["start_yr", "end_yr", "year", "fx", "dx"],
        )
        if index == 0:
            fx_dx_info = row
            index += 1
        else:
            fx_dx_info = pd.concat([fx_dx_info, row])

    # for loop should end here

    # Now add the extra or meta information to the data frame containing
    # the results. There is most likely a more efficient way to do this
    # in python but for now create an placeholder index column to join
    # the meta data (1 row of data) to the output data.
    extra_info["i"] = 1
    fx_dx_info["i"] = 1
    out = pd.merge(left=extra_info, right=fx_dx_info, left_on="i", right_on="i")
    out = out.loc[:, out.columns != "i"].copy()

    # Make sure that the objects being returned have int values for the
    # years instead of returning those values as a factor. This makes it
    # easier to work with data objects returned from other functions.
    data_types_dict = {"start_yr": "int32", "year": "int32", "end_yr": "int32"}
    out = out.astype(data_types_dict)

    return out


def subset_archive(staggered_archive, end_yr_vector):
    """
    Subset a staggered archive to entries with `end_yr` in `end_yr_vector`.

    This function takes a staggered archive with chunked data for a 9-year window
    following each year in 1850-2100 and subsets it to the entries with `end_yr`
    in `end_yr_vector`.

    :param staggered_archive: A formatted archive with chunked data starting in each year.
    :type staggered_archive: pandas.DataFrame
    :param end_yr_vector: Vector of end years to subset the archive to.
    :type end_yr_vector: list or similar iterable
    :return: A pandas DataFrame of the subsetted archive, same format but fewer entries.
    :rtype: pandas.DataFrame
    """
    out = (
        staggered_archive[staggered_archive["end_yr"].isin(end_yr_vector)]
        .reset_index(drop=True)
        .copy()
    )
    return out
