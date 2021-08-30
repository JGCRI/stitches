# Define the functions that are used to process raw tas time series
# into the "chunks" of data that is used in the matching process.

# Import packages
import stitches.fx_util as util
from sklearn.linear_model import LinearRegression
import math as math
import numpy as np
import pandas as pd


def calculate_rolling_mean(data, size):
    """"
  Calculate the rolling mean for the data frame with a user defined size centered window.
  :param data:        A data frame of the cmip absolute temperature
  :type data:        pandas.core.frame.DataFrame
  :param size:          An integer value for the size of the window to use when calculating the rolling mean
  :type size:           int
  :return:          A pandas data frame of the smoothed time series (rolling mean applied)
  """
    # Check inputs
    util.check_columns(data, {'model', 'experiment', 'ensemble', 'year', 'value', 'variable'})

    # Index the data frame by the year, so that the rolling mean respects the years
    data.index = data['year']

    group_by = ['model', 'experiment', 'ensemble', 'variable']

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
    rslt['rollingAvg'] = rslt.groupby(group_by)['value'].transform(
        lambda x: x.rolling(size, center=True, min_periods=1).mean())
    # ^ seems like the way people usually get the functionality of a
    # dplyr group_by %>% mutate()  call.

    # rename so that value is the smoothed data:
    rslt = rslt.drop('value', 1).rename(columns={'rollingAvg': 'value'}).reset_index(drop=True)

    return rslt


def chunk_ts(df, n):
    """Format a data frame into an array of data frames containing data for n-sized years of successive data.
  :param df:     data frame of climate data to chunk into different periods
  :type df:     pandas DataFrame
  :param n:    the size of the windows to chunk into separate periods
  :type n:    int
  :return:    pandas DataFrame identical to df with the addition of a chunk column
  # TODO hmmm do we want this function to also work on ESM data? If so do we need to change this function??
  # TODO does it need to be for only one variable?
  # TODO do we like chunks or are these windows?
  # TODO do we really need the year column?
  """

    # Check inputs
    df = df.drop_duplicates().reset_index(drop=True).copy()
    util.check_columns(df, {'year', 'variable', 'value'})
    if not (len(df["variable"].unique()) == 1):
        raise TypeError(f'Multiple variables discovered in "{df}"')

    # TODO How do we want to handle when the length of the time series cannot be split up into even chunks?
    # Add a column of data that categorizes the data into the different periods, right now we allow for
    # a list that is smaller than n at the end.
    # Save a copy of the length of the list
    yr_len = len(df["year"].unique())

    # Determine the number of unique n sized chunks, right now we assume that at most a chunk
    # can have n entries or less.
    n_chunks = math.ceil(yr_len / n)
    # Make a np array of the chunk labels, where each label repeates n times.
    chunk_labels = np.repeat(list(range(0, n_chunks)), n)
    # Subset the chunk list so that matches the length of the data frame.
    chunk_labels = chunk_labels[range(0, yr_len)]

    df["chunk"] = chunk_labels

    return df


def get_chunk_info(df):
    """ Determine the value and the rate of change for each chunk.
  :param df:     data frame of climate data chunked into different periods
  :type df:     pandas DataFrame
  :return:    pandas DataFrame of the chunk information, the start and end years as well as the chunk value (fx)
  and the chunk rate of change (dx).
  """

    # Check the inputs
    # TODO where/why is this index column appearing it is annoying & causing problems bleh!
    flag = 'index' in df.columns
    if flag:
        del df['index']

    util.check_columns(df, {'year', 'variable', 'value', 'chunk'})
    if not (len(df["variable"].unique()) == 1):
        raise TypeError(f'Multiple variables discovered in "{df}"')

    # Save information that will be added to
    extra_columns = set(df.columns).difference({"chunk", "value", "year"})
    extra_info = df[extra_columns].drop_duplicates()
    if not (len(extra_info) == 1):
        raise TypeError(f'more than 1 type of data being read into the function')

    # Group the data frame by the chunk label so that we can use a for loop
    # to extract information from each chunk of data.
    df_gby = df.groupby('chunk')

    # Make an empty data frame that to store the chunk data.
    fx_dx_info = pd.DataFrame(columns=["start_yr", "end_yr", "year", "fx", "dx"])

    # Use the for loop to work our way through the different chunks/periods of
    # data, gathering information about each start and stop year, the central
    # value, and the rate of change.
    for key, chunk in df_gby:
        # Save some information about the period of data, when it starts and stops.
        chunk["year"] = chunk["year"].astype(int)
        start_yr = min(chunk["year"])
        end_yr = max(chunk["year"])

        # how do we want to address if is no single middle year because the length of the
        # chunks is even?
        # Get the fx, the value of the center of the time period,
        # TODO or should it be the median value over the time span?
        x = math.ceil(np.median(chunk["year"]))  # right now we select the one from the high year
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
        row = pd.DataFrame([[start_yr, end_yr, x, fx, dx]], columns=["start_yr", "end_yr", "year", "fx", "dx"])
        fx_dx_info = fx_dx_info.append(row)

    # for loop should end here

    # Now add the extra or meta information to the data frame containing
    # the results. There is most likely a more effecient way to do this
    # in python but for now create an placeholder index column to join
    # the meta data (1 row of data) to the output data.
    extra_info["i"] = 1
    fx_dx_info["i"] = 1
    out = pd.merge(left=extra_info, right=fx_dx_info, left_on='i', right_on='i')
    out = out.loc[:, out.columns != 'i'].copy()

    # Make sure that the objects being returned have int values for the
    # years instead of returning those values as a factor. This makes it
    # easier to work with data objects returned from other functions.
    data_types_dict = {'start_yr': 'int32', 'year': 'int32', 'end_yr': 'int32'}
    out = out.astype(data_types_dict)

    return out
