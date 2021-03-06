from stitches.pkgimports import *

###############################################################################
# Contains first draft of the functions used to match up, note that there are
# some helper functions that will most likely be deprecated over time.
# But I was struggling to figure out how to have functions work together
# that are defined in different py scripts :(


def cleanup_main_tgav(f):
  """ This function cleans up the main_tgav_all_pangeo_list_models.csv to prepare it for the match up, temp use while
    the archive is still under development.

    :param f:     The path to the file to import, should be ./stitches/data/created_data/main_tgav_all_pangeo_list_models.csv
    :param save_individ_tgav:    True/False about whether to save the individual ensemble's Tgav in its own file.

    :return:          A formatted data frame of Tgav time series for the netcdf file, including metadata.

    TODO remove this function when no longer needed.
    """
  if not (os.path.isfile(f)):
    raise TypeError(f"file does not exist")

  if(f[len(f) - 3 : ] == 'csv'):
    df = pd.read_csv(f)
  elif(f[len(f) - 3 : ] == 'dat'):
    with open(f, 'rb') as f1:
      # Pickle the 'data' dictionary using the highest protocol available.
      df = pickle.load(f1)
  else:
    raise TypeError(f"unsupported file type, must be csv or dat")


  # Select the columns containing actual data, removing the index.
  df = df[['activity', 'model', 'experiment', 'ensemble_member', 'timestep', 'grid_type', 'file', 'year', 'tgav']]
  # Rename columns and add a variable column
  df = df.rename(columns={'ensemble_member':'ensemble', 'tgav':'value'})
  df.columns = ['activity', 'model', 'experiment', 'ensemble', 'timestep',
         'grid_type', 'file', 'year', 'value']
  df["variable"] = "tgav"
  
  # sort to clean up year ordering
  # TODO go back into the script where we create the csv of Tgav values and figure out why MPI-LR has the years
  # get out of order (there's only one netcdf per ensemble member so it isn't that).
  df = df.sort_values(by=['activity', 'model', 'experiment', 'ensemble', 'timestep','grid_type', 'file', 'year']).copy()

  return df

#########################################################################################################

def select_model_to_emulate(model_name, df):
  """ Convert the temp data from absolute into an anomaly
  :param model_name:     A str name of the model to emulate
  :type data:        str
  :param df:         A data frame of the tgav data.
  :type df:         pandas data frame

  :return:          A pandas data frame of cmip tgav for a single model

  # TODO hate the function name, wanted something easier to work with
  """

  if not (isinstance(df, pd.DataFrame)):
    raise TypeError(f"df not imported")

  if not (df.size >= 10):
    raise TypeError(f"df is empty")

  # Check to make sure that the model name is within the data frame.
  if not (model_name in set(df["model"].unique())):
    raise TypeError(f"model_name tgav data not avaiable")

  # Now subset the data for a single model to emulate.
  out = df[df["model"] == model_name].copy()
  return out

#########################################################################################################
def calculate_anomaly(data, startYr = 1995, endYr = 2014):

  """Convert the temp data from absolute into an anomaly relative to a reference period.

  :param data:        A data frame of the cmip absolute temperature
  :type data:        pandas.core.frame.DataFrame
  :param startYr:      The first year of the reference period, default set to 1995 corresponding to the
  IPCC defined reference period.
  :type startYr:       int
  :param endYr:       The final year of the reference period, default set to 2014 corresponding to the
  IPCC defined reference period.
  :type endYr:        int

  :return:          A pandas data frame of cmip tgav as anomalies relative to a time-averaged value from
  a reference period, default uses a reference period form 1995-2014
  """
  # inputs
  req_cols = {'activity', 'model', 'experiment', 'ensemble', 'timestep', 'grid_type',
        'file', 'year', 'value', 'variable'}
  col_names = set(data.columns)
  if not (req_cols.issubset(col_names)):
    raise TypeError(f'Missing columns from "{data}".')

  to_use = data[['model', 'experiment', 'ensemble', 'year', 'value']].copy()

  # Calculate the average value for the reference period defined
  # by the startYr and ednYr arguments.
  # The default reference period is set from 1995 - 2014.
  #
  # Start by subsetting the data so that it only includes values from the specified reference period.
  subset_data = to_use[to_use['year'].between(startYr, endYr)]

  # Calculate the time-averaged reference value for each ensemble
  # realization. This reference value will be used to convert from absolute to
  # relative temperature.
  reference_values = subset_data.groupby(['model', 'ensemble']).agg(
    {'value': lambda x: sum(x) / len(x)}).reset_index().copy()
  reference_values = reference_values.rename(columns={"value": "ref_values"})

  # Combine the dfs that contain the absolute temperature values with the reference values.
  # Then calculate the relative temperature.
  merged_df = data.merge(reference_values, on=['model', 'ensemble'], how='left')
  merged_df['value'] = merged_df['value'] - merged_df['ref_values']
  merged_df = merged_df.drop(columns='ref_values')

  return merged_df

#########################################################################################################

def paste_historical_data(input_data):
  """"
  Paste the appropriate historical data into each future scenario so that SSP585 realization 1, for
  example, has the appropriate data from 1850-2100.

  :param input_data:        A data frame of the cmip absolute temperature
  :type input_data:        pandas.core.frame.DataFrame

  :return:          A pandas data frame of the smoothed time series (rolling mean applied)
  """
  # TODO add some code that checks the inputs of the input_data
  # Relabel the historical values so that there is a continuous rolling mean between the
  # historical and future values.
  # #######################################################
  # Subset the historical data
  historical_data = input_data[input_data["experiment"] == "historical"].copy()

  # Create a subset of the future data
  future_data = input_data[input_data["experiment"] != "historical"].copy()
  future_scns = set(future_data["experiment"].unique())

  frames = []
  for scn in future_scns:
    d = historical_data.copy()
    d["experiment"] = scn
    frames.append(d)

  frames.append(future_data)
  data = pd.concat(frames)

  return(data)

#########################################################################################################

def calculate_rolling_mean(input_data, size):
  """"
  Calculate the rolling mean for the data frame with a user defined size centered window.

  :param input_data:        A data frame of the cmip absolute temperature
  :type input_data:        pandas.core.frame.DataFrame
  :param size:          An integer value for the size of the window to use when calculating the rolling mean
  :type size:           int

  :return:          A pandas data frame of the smoothed time series (rolling mean applied)
  """
  # TODO add some code that checks the inputs of the input_data
  # inputs
  req_cols = {'model', 'experiment', 'ensemble', 'year', 'value', 'variable'}
  col_names = set(input_data.columns)
  if not (req_cols.issubset(col_names)):
    raise TypeError(f'Missing columns from "{input_data}".')

  req_exps = {"historical"}
  exp_set = set(input_data['experiment'].unique())

  if not (req_exps.issubset(exp_set)):
    raise TypeError(f'{input_data} is missing data for the historical experiment.')

  # Relabel the historical values so that there is a continuous rolling mean between the
  # historical and future values.
  data = paste_historical_data(input_data)

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

  """ Format a data frame into an array of data frames containing data for n-sized years of successive data.

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

  # Check the inputs
  req_cols = {'year', 'variable', 'value'}
  col_names = set(df.columns)
  if not (req_cols.issubset(col_names)):
    raise TypeError(f'Missing columns from "{df}".')
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
  req_cols = {'year', 'variable', 'value', 'chunk'}
  col_names = set(df.columns)
  if not (req_cols.issubset(col_names)):
    raise TypeError(f'Missing columns from "{df}".')
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
    start_yr = min(chunk["year"])
    end_yr = max(chunk["year"])

    # how do we want to address if is no single middle year because the length of the
    # chunks is even?
    # Get the fx, the value of the center of the time period,
    # TODO or should it be the median value over the time span?
    x = math.ceil(np.median(chunk["year"])) # right now we select the one from the high year
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
  out = pd.merge(left = extra_info, right = fx_dx_info, left_on='i', right_on='i')
  out = out.loc[:, out.columns != 'i'].copy()

  # Make sure that the objects being returned have int values for the
  # years instead of returning those values as a factor. This makes it
  # easier to work with data objects returned from other functions.
  data_types_dict = {'start_yr': 'int32', 'year': 'int32', 'end_yr': 'int32'}
  out = out.astype(data_types_dict)

  return out
