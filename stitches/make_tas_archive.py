# Define the functions used to get Get the weighted global mean temperature
# from pangeo CMIP6 results.

# Import packages
import stitches.fx_pangeo as pangeo
import stitches.fx_data as data
import stitches.fx_util as util
import os
import pkg_resources
import pandas as pd


def get_global_tas(path):
    """
    Calculate the weighted annual global mean temp.

    :param path:  a zstore path to the CMIP6 files stored on pangeo.
    :type path:   str

    :return:      str path to the location of file containing the weighted global mean.
    """

    # Make the name of the output file that will only be created if the output
    # does not already exists.
    # TODO CVR is there a way to make this with a pkg resources call? Important note
    # TODO the temp dir should not be exported with the package & not all users will
    # TODO will be generating an archive from scratch most of the users will be using
    # TODO the archive internal to package data.
    temp_dir = "stitches/data/temp-data"
    if os.path.isdir(temp_dir) == False:
        os.mkdir(temp_dir)

    ofile = temp_dir + "/" + path.replace("/", "_") + "temp.dat"
    flag = os.path.isfile(ofile)
    if flag is False:

        # Download the CMIP data & calculate the weighted annual global mean .
        d = pangeo.fetch_nc(path)
        global_mean = data.global_mean(d)
        annual_mean = global_mean.coarsen(time=12).mean()

        # Format the CMIP meta data & global means, then combine into a single data frame.
        meta = data.get_ds_meta(d)
        t = annual_mean["time"].dt.strftime("%Y%m%d").values
        year = list(map(lambda x: util.selstr(x, start=0, stop=4), t))

        val = annual_mean["tas"].values
        d = {'year': year, 'value': val}
        df = pd.DataFrame(data=d)
        out = util.combine_df(meta, df)

        # Write the output
        out.to_pickle(ofile)
        return ofile
    else:
        return ofile


def calculate_anomaly(data, startYr=1995, endYr=2014):
    """
    Convert the temp data from absolute into an anomaly relative to a reference period.

    :param data:        A data frame of the cmip absolute temperature
    :type data:        pandas.core.frame.DataFrame
    :param startYr:      The first year of the reference period, default set to 1995 corresponding to the IPCC defined reference period.
    :type startYr:       int
    :param endYr:       The final year of the reference period, default set to 2014 corresponding to the IPCC defined reference period.
    :type endYr:        int

    :return:          A pandas data frame of cmip tgav as anomalies relative to a time-averaged value from a reference period, default uses a reference period form 1995-2014
    """

    # Inputs
    util.check_columns(data, {'variable', 'experiment', 'ensemble', 'model', 'year', 'value'})
    to_use = data[['model', 'experiment', 'ensemble', 'year', 'value']].copy()

    # Calculate the average value for the reference period defined
    # by the startYr and ednYr arguments.
    # The default reference period is set from 1995 - 2014.
    #
    # Start by subsetting the data so that it only includes values from the specified reference period.
    to_use["year"] = to_use["year"].astype(int)
    to_use = to_use[to_use["experiment"] == "historical"]
    subset_data = to_use[to_use['year'].between(startYr, endYr)]

    # Calculate the time-averaged reference value for each ensemble
    # realization. This reference value will be used to convert from absolute to
    # relative temperature.
    reference_values = subset_data.groupby(['model', 'ensemble']).agg(
        {'value': lambda x: sum(x) / len(x)}).reset_index().copy()
    reference_values = reference_values.rename(columns={"value": "ref_values"})

    # Combine the dfs that contain the absolute temperature values with the reference values.
    # Then calculate the relative temperature.
    merged_df = data.merge(reference_values, on=['model', 'ensemble'], how='inner')
    merged_df['value'] = merged_df['value'] - merged_df['ref_values']
    merged_df = merged_df.drop(columns='ref_values')

    return merged_df


def paste_historical_data(input_data):
    """"
    Paste the appropriate historical data into each future scenario so that SSP585 realization 1, for
    example, has the appropriate data from 1850-2100.

    :param input_data:        A data frame of the cmip absolute temperature
    :type input_data:        pandas.core.frame.DataFrame

    :return:          A pandas data frame of the smoothed time series (rolling mean applied)
    """

    # Relabel the historical values so that there is a continuous rolling mean between the
    # historical and future values.
    # #######################################################
    # Create a subset of the non historical & future scns
    other_exps = ['1pctCO2', 'abrupt-2xCO2', 'abrupt-4xCO2']
    other_data = input_data[input_data["experiment"].isin(other_exps)]

    # Subset the historical data
    historical_data = input_data[input_data["experiment"] == "historical"].copy()

    # Create a subset of the future data
    fut_exps = ['ssp126', 'ssp245', 'ssp370', 'ssp585', 'ssp534-over', 'ssp119', 'ssp434', 'ssp460']
    future_data = input_data[input_data["experiment"].isin(fut_exps)]
    future_scns = set(future_data["experiment"].unique())

    frames = []
    for scn in future_scns:
        d = historical_data.copy()
        d["experiment"] = scn
        frames.append(d)

    frames.append(future_data)
    frames.append(other_data)
    data = pd.concat(frames)

    # TODO is there a better way to prevent duplicates in 2015 & 2016 values?
    d = data.groupby(['variable', 'experiment', 'ensemble', 'model', 'year'])['value'].agg('mean').reset_index()

    return d

def make_tas_archive():
    """"
    #  The function that creates the archive

    :return:          Array of the tas files created.
    """
    # Get tas data & calculate global mean temp

    # Get the pangeo table of contents.
    df = pangeo.fetch_pangeo_table()

    # Subset the monthly tas data, these are the files that we will want to process
    # for the tas archive.
    xps = ["historical", "1pctCO2", "abrupt-4xCO2", "abrupt-2xCO2", "ssp370", "ssp245", "ssp119",
           "ssp434", "ssp460", "ssp126", "ssp585", "ssp534-over"]
    df = df.loc[(df["experiment_id"].isin(xps)) &  # experiments of interest
                (df["table_id"] == "Amon") &  # monthly data
                (df["grid_label"] == "gn") &  # we are only interested in the results returned in the native
                (df["variable_id"] == "tas") &  # select temperature data
                (df['member_id'].str.contains('p1'))]  # select only the members of the p1 physics group

    # For each of the CMIP6 files to calculate the global mean temperature and write the
    # results to the temporary directory.
    xx = list(map(get_global_tas, df.zstore.values))

    # Clean Up & Quality Control
    #
    # Find all of the files and read in the data, store as a single data frame.
    # TODO CRV is there a way to make this path relative even though it may not
    # exist in the exported package?
    raw_data = util.load_data_files('data/temp-data')

    # Note that the first three steps only apply to the historical & ssp experiments,
    # the idealized experiments do not need to go through these steps.
    #
    # First round of cleaning check the historical dates.
    # Make sure that the historical run starts some time
    # before 1855 & that that it runs until 2014.
    # Subset the Hector historical
    his_info = (raw_data.loc[(raw_data["experiment"] == "historical")]
                .groupby(["model", "experiment", "ensemble"])["year"]
                .agg(["min", "max"]).reset_index())
    his_info["min"] = his_info["min"].astype(int)
    his_info["max"] = his_info["max"].astype(int)

    # Make sure the start date is some times before 1855.
    start_yr = his_info[his_info["min"] > 1855].copy()
    to_remove = start_yr[["model", "experiment", "ensemble"]]

    # Make sure that all of historical have data up until 2014
    end_yr = his_info[his_info["max"] < 2014].copy()
    to_remove = to_remove.append(end_yr[["model", "experiment", "ensemble"]])
    clean_d1 = util.join_exclude(raw_data, to_remove)

    # Second round of cleaning check the future dates.
    # Make sure that the future scenarios start at 2015 & run beyond 2100.
    fut_exps = ['ssp245', 'ssp126', 'ssp585', 'ssp119', 'ssp370', 'ssp434', 'ssp534-over', 'ssp460']
    fut_info = (clean_d1.loc[(clean_d1["experiment"].isin(fut_exps))]
                .groupby(["model", "experiment", "ensemble"])["year"]
                .agg(["min", "max"]).reset_index())
    fut_info["min"] = fut_info["min"].astype(int)
    fut_info["max"] = fut_info["max"].astype(int)

    # If the future scenario starts after 2015 drop it.
    start_yr = fut_info[fut_info["min"] > 2015].copy()
    to_remove = start_yr[["model", "experiment", "ensemble"]]

    # Make sure the future scenario runs until 2050 otherwise drop it.
    end_yr = fut_info[fut_info["max"] < 2050].copy()
    to_remove = to_remove.append(end_yr[["model", "experiment", "ensemble"]])
    clean_d2 = util.join_exclude(clean_d1, to_remove)

    # Third round of clean up
    # Make sure that there is data from the historical experiment for each ensemble member with
    # future results.
    exp_en_mod = clean_d2[["experiment", "ensemble", "model"]].drop_duplicates().copy()

    # Separate the data frame of the experiment / ensemble / model information into
    # the historical and non historical experiments.
    hist_ensemble = (exp_en_mod.loc[exp_en_mod["experiment"] == "historical"][["model", "ensemble"]]
                     .drop_duplicates()
                     .copy())
    non_hist_ensemble = (exp_en_mod[exp_en_mod["experiment"] != "historical"][["model", "ensemble"]]
                         .drop_duplicates()
                         .copy())
    # use an inner join to select the historical ensemble that have future results as well as
    # the future results have have historical results.
    to_keep = non_hist_ensemble.merge(hist_ensemble, how="inner", on=["ensemble", "model"])

    # Update the raw data table to only include the model / ensembles members that have both a
    # historical and non historical ensemble realization.
    clean_d3 = clean_d2.merge(to_keep, how="inner")

    # Before the fourth round of clean up add back in the idealized experiment results.
    idealized_exps = {'1pctCO2', 'abrupt-2xCO2', 'abrupt-4xCO2'}
    idealized_dat = raw_data.loc[raw_data['experiment'].isin(idealized_exps)]
    clean_d3 = pd.concat([clean_d3, idealized_dat])

    # Fourth round of cleaning make sure there are no missing dates.
    yrs = (clean_d2.groupby(["model", "experiment", "ensemble"])["year"]
           .agg(["min", "max", "count"])
           .reset_index())
    yrs["min"] = yrs["min"].astype(int)
    yrs["max"] = yrs["max"].astype(int)
    yrs["count"] = yrs["count"].astype(int)
    yrs["diff"] = (yrs["max"] - yrs["min"]) + 1
    yrs["diff"] = yrs["diff"].astype(int)
    to_remove = yrs[yrs["diff"] != yrs["count"]]
    clean_d4 = util.join_exclude(clean_d3, to_remove)

    # Order the data frame to make sure that all of the years are in order.
    cleaned_data = (clean_d4.sort_values(by=['variable', 'experiment', 'ensemble', 'model', 'year'])
                    .reset_index(drop=True))

    # Format Data
    #
    # In this section convert from absolute value to an anomaly & concatenate the historical data
    # with the future scenarios.
    data_anomaly = calculate_anomaly(cleaned_data)
    data = paste_historical_data(data_anomaly)
    data = data.sort_values(by=['variable', 'experiment', 'ensemble', 'model', 'year'])
    data = data[["variable", "experiment", "ensemble", "model", "year", "value"]].reset_index(drop=True)

    # Add the z store values to the data frame.
    # Get the pangeo table of contents & assert that the data frame exists and contains information.
    df = pangeo.fetch_pangeo_table()
    df = df.loc[(df["table_id"] == "Amon") &  # monthly data
                (df["grid_label"] == "gn") &  # we are only interested in the results returned in the native
                (df["variable_id"] == "tas") &  # select temperature data
                (df['member_id'].str.contains('p1'))].copy()  # select only the members of the p1 physics group

    if len(df) <= 0:
        raise Exception('Unable to connect to pangeo, make sure to disconnect from VP')

    # Format the pangeo data frame so that it reflects the contents of data.
    pangeo_df = df[["source_id", "experiment_id", "member_id", "variable_id", "zstore"]].drop_duplicates()
    pangeo_df = pangeo_df.rename(columns={"source_id": "model", "experiment_id": "experiment",
                                          "member_id": "ensemble", "variable_id": "variable"})

    # Add the zstore file information to the data frame via a left join.
    data = data.merge(pangeo_df, on=['variable', 'experiment', 'ensemble', 'model'], how="inner")

    # Modify the zstore path names to replace the future scn string with historical.
    # TODO replace this for loop it is pretty slow
    new_zstore = []
    for i in data.index:
        # Select the row from the data frame.
        row = data.loc[i]

        # Check to see if the zstore needs to be changed based on if it is a future experiment.
        fut_exps = set(['ssp119', 'ssp126', 'ssp245', 'ssp370', 'ssp434', 'ssp460', 'ssp534-over', 'ssp585'])
        change = row["experiment"] in fut_exps
        if change:
            new = row["zstore"].replace(row["experiment"], "historical")
        else:
            new = row["zstore"]

        new_zstore.append(new)

    data["zstore"] = new_zstore

    # Save a copy of the tas values, these are the value that will be used to get the
    # tas data chunks. Note that this file has to be compressed so will need to read in
    # using pickle_utils.load()

    files = []
    tas_data_dir = pkg_resources.resource_filename('stitches', 'data/tas-data')
    os.mkdir(tas_data_dir)
    for name, group in data.groupby(['model']):
        path = tas_data_dir + '/' + name + '_tas.csv'
        files.append(path)
        group.to_csv(path, index=False)

    return files
