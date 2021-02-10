from stitches.pkgimports import *

###############################################################################
# Contains first draft of the functions used to match up, note that there are
# some helper functions that will most likely be deprecated over time.
# But I was struggling to figure out how to have functions work together
# that are defined in different py scripts :(


def cleanup_main_tgav(f):
    """ This function cleans up the main_tgav_all_pangeo_list_models.csv to prepare it for match up, temp use while
       the archive is still under devlopment.

        :param f:         The path to the file to import, should be ./stitches/data/created_data/main_tgav_all_pangeo_list_models.csv
        :param save_individ_tgav:       True/False about whether to save the individual ensemble's Tgav in its own file.

        :return:                    A formatted data frame of Tgav time series for the netcdf file, including metadata.

        TODO remove this fuction when no longer needed.
       """
    if not (os.path.isfile(f)):
        raise TypeError(f"file does not exist")

    df = pd.read_csv(f)
    # Select the columns containing actual data, removing the index.
    df = df[['activity', 'model', 'experiment', 'ensemble_member', 'timestep', 'grid_type', 'file', 'year', 'tgav']]
    # Rename columns and add a variable column
    df = df.rename(columns={'ensemble_member':'ensemble', 'tgav':'value'})
    df.columns = ['activity', 'model', 'experiment', 'ensemble', 'timestep',
                  'grid_type', 'file', 'year', 'value']
    df["variable"] = "tgav"
    return df

#########################################################################################################

def select_model_to_emulate(model_name, df):
    """ Convert the temp data from absolute into an anomaly
    :param model_name:         A str name of the model to emulate
    :type data:                str
    :param df:                 A data frame of the tgav data.
    :type df:                  pandas data frame

    :return:                   A pandas data frame of cmip tgav for a single model

    # TODO hate the function name, wanted something that was easier to work with
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
def calculate_anomaly(data, startYr  = 1995, endYr = 2014):

    """Convert the temp data from absolute into an anomaly relative to a reference period.

    :param data:               A data frame of the cmip absolute temperature
    :type data:                pandas.core.frame.DataFrame
    :param startYr:            The first year of the reference period, default set to 1995 corresponding to the
    IPCC defined reference period.
    :type startYr:             int
    :param endYr:              The final year of the reference period, default set to 2014 corresponding to the
    IPCC defined reference period.
    :type endYr:                int

    :return:                   A pandas data frame of cmip tgav as anomalies relative to a time-averaged value from
    a reference period, default ues a reference period form 1995-2014
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

def calculate_rolling_mean(input_data, size):
    """"
    Calculate the rolling mean for the data frame

    :param input_data:               A data frame of the cmip absolute temperature
    :type input_data:                pandas.core.frame.DataFrame
    :param size:                    An integer value for the size of the window to use when calculating the rolling mean
    :type size:                     int

    :return:                   A pandas data frame of the smoothed time series (rolling mean applied)
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

    # Index the data frame by the year, so that the rolling mean respects the years
    data.index = data['year']
    # Now calculate the rolling mean, make sure that the data frame is properly sorted by the year value.
    group_by = ['model', 'experiment', 'ensemble', 'variable']
    rslt = data.groupby(group_by)['value'].rolling(size).mean().reset_index()

    return rslt
