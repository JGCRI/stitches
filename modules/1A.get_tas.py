# Get the weighted global mean temperature for CMIP6 results.
# #############################################################################
# General setup
# #############################################################################
# Import packages
import stitches.fx_pangeo as pangeo
import stitches.fx_data as data
import stitches.fx_util as util
import os
import pandas as pd
import pickle_utils as pickle

# Define module specific functions
def get_global_tas(path):
       """ Calculate the weighted global mean temp.

       :param path:  a zstore path to the CMIP6 files stored on pangeo.

       :return:      str path to the location of file containing the weighted global mean.
       """

       # Make the name of the output file that will only be created if the output
       # does not already exists.
       temp_dir = "stitches/data/temp-data" # is there some whay to make this file if it does not exist?
       ofile = temp_dir + "/" + path.replace("/", "_") + "temp.dat"
       flag = os.path.isfile(ofile)
       if flag is False:
              print("processing data")

              # Download the CMIP data & calculate the weighted mean.
              d = pangeo.fetch_nc(path)
              mean = data.global_mean(d)

              # Format the CMIP meta data & global means, then combine into a single data frame.
              meta = data.get_ds_meta(d)
              t = mean["time"].dt.strftime("%Y%m%d").values
              val = mean["tas"].values
              d = {'time': t, 'value': val}
              df = pd.DataFrame(data=d)
              out = util.combine_df(meta, df)

              # Wrtie the output
              out.to_pickle(ofile)
              return ofile

       if flag is True:
              return ofile


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
  # TODO add some code that checks the inputs of the input_data
  # Relabel the historical values so that there is a continuous rolling mean between the
  # historical and future values.
  # #######################################################
  # Create a subset of the non historical & future scns
  # TODO is there a way to make this more robust so if there is more exps get added they are accounted for?
  other_exps = ['1pctCO2', 'abrupt-2xCO2', 'abrupt-4xCO2']
  other_data = input_data[input_data["experiment"].isin(other_exps)]

  # Subset the historical data
  historical_data = input_data[input_data["experiment"] == "historical"].copy()

  # Create a subset of the future data
  fut_exps = ['ssp126', 'ssp245', 'ssp370', 'ssp585', 'ssp534-over', 'ssp119', 'ssp434']
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


#############################################################################################
# 1. Get tas Data
#############################################################################################
# Get the pangeo table of contents.
df = pangeo.fetch_pangeo_table()

# Subset the monthly tas data, these are the files that we will want to process
# for the tas archive.
xps = ["historical", "1pctCO2", "abrupt-4xCO2", "abrupt-2xCO2", "ssp370", "ssp245", "ssp119",
       "ssp434", "ssp460", "ssp126", "ssp585", "ssp534-over"]
df = df[df["experiment_id"].isin(xps)]  # experiments of interest
df = df[df["table_id"] == "Amon"]   # monthly data
df = df[df["grid_label"] == "gn"]   # we are only interested in the results returned in the native grid
df = df[df["variable_id"] == "tas"]       # select temperature data

# For each of the CMIP6 files to calculate the global mean temperature and write the
# results to the temporary directory.
for f in df.zstore.values[range(0,10)]:
       get_global_tas(f)


#############################################################################################
# 2. Clean Up & Quality Control
#############################################################################################
# Find all of the files and read in the data, store as a single data frame.
files_to_process = util.list_files("stitches/data/temp-data")

raw_data = []
for f in files_to_process:
       d = pd.read_pickle(f)
       raw_data.append(d)
# Convert into a single data frame.
raw_data = pd.concat(raw_data)

# First round of cleaning check the historical dates.
# Make sure that the historical run starts some time
# before 1855 & that that it runs until 2014.
# Subset the Hector historical
his_info = raw_data[raw_data["experiment"] == "historical"].groupby(["model", "experiment", "ensemble"])["year"].agg(["min", "max"]).reset_index()
his_info["min"] = his_info["min"].astype(int)
his_info["max"] = his_info["max"].astype(int)

# Make sure the start date is some times before 1855.
start_yr = his_info[his_info["min"] > 1855]
to_remove=start_yr[["model", "experiment", "ensemble"]]

# Make sure that all of historical have data up until 2014
end_yr = his_info[his_info["max"] < 2014]
to_remove = to_remove.append(end_yr[["model", "experiment", "ensemble"]])
clean_d1 = util.join_exclude(raw_data, to_remove)

# Second round of cleaning check the future dates.
# Make sure that the future scenarios start at 2015 & run beyond 2100.
fut_exps = ['ssp245', 'ssp126', 'ssp585', 'ssp119', 'ssp370', 'ssp434', 'ssp534-over', 'ssp460']
fut_info = clean_d1[clean_d1["experiment"].isin(fut_exps)].groupby(["model", "experiment", "ensemble"])["year"].agg(["min", "max"]).reset_index()
fut_info["min"] = fut_info["min"].astype(int)
fut_info["max"] = fut_info["max"].astype(int)

# If the future scenario starts after 2015 drop it.
start_yr = fut_info[fut_info["min"] > 2015]
to_remove = start_yr[["model", "experiment", "ensemble"]]

# Make sure the future scenario runs until 2100 otherwise drop it.
end_yr = fut_info[fut_info["max"] < 2100]
to_remove = to_remove.append(end_yr[["model", "experiment", "ensemble"]])
clean_d2 = util.join_exclude(clean_d1, to_remove)

# Third round of cleaning make sure there are no missing dates.
yrs = clean_d2.groupby(["model", "experiment", "ensemble"])["year"].agg(["min", "max", "count"]).reset_index()
yrs["min"] = yrs["min"].astype(int)
yrs["max"] = yrs["max"].astype(int)
yrs["count"] = yrs["count"].astype(int)
yrs["diff"] = (yrs["max"] - yrs["min"]) + 1
yrs["diff"] = yrs["diff"].astype(int)
to_remove = yrs[yrs["diff"] != yrs["count"]]
clean_d3 = util.join_exclude(clean_d2, to_remove)


# Fourth round of clean up
# Make sure that there is data from the historical experiment for each ensemble member with
# future results.
exp_en_mod = clean_d3[["experiment", "ensemble", "model"]].drop_duplicates()

# Separate the data frame of the experiment / ensemble / model information into
# the historical and non historical experiments.
hist_ensemble = exp_en_mod[exp_en_mod["experiment"] == "historical"][["model", "ensemble"]].drop_duplicates()
non_hist_ensemble = exp_en_mod[exp_en_mod["experiment"] != "historical"][["model", "ensemble"]].drop_duplicates()
non_hist_ensemble['keep'] = 1
to_keep = hist_ensemble.merge(non_hist_ensemble, how="inner", on=["ensemble", "model"])
to_remove = to_keep[to_keep["keep"] != 1][["model", "ensemble"]]

# Update the raw data table to only include the model / ensembles members that have both a
# historical and non historical ensemble realization.
clean_d4 = util.join_exclude(clean_d3, to_remove)

# Order the data frame to make sure that all of the years are in order.
cleaned_data = clean_d4.sort_values(by=['variable', 'experiment', 'ensemble', 'model', 'year'])

#############################################################################################
# 3. Format Data
#############################################################################################
# In this section convert from absolute value to an anomaly & concatenate the historical data
# with the future scenarios.
data_anomaly = calculate_anomaly(cleaned_data)
data = paste_historical_data(data_anomaly)
data = data.sort_values(by=['variable', 'experiment', 'ensemble', 'model', 'year'])
data = data[["variable", "experiment", "ensemble", "model", "year", "value"]].reset_index(drop=True)

# TODO we need to figure out how to add the zstore file column to the data frame!
# this set up does not really work in attempt to try and add the zstore column
# with the correct historical path ends up duplicating entries.
if False:
    # Add the z store values to the data frame.
    # Get the pangeo table of contents & assert that the data frame exists and contains information.
    df = pangeo.fetch_pangeo_table()
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

#############################################################################################
# 4. Save
#############################################################################################
# Save a copy of the tas values, these are the value that will be used to get the
# tas data chunks. Note that this file has to be compressed so will need to read in
# using pickle_utils.load()
pickle.dump(data, "stitches/data/tas_values.pkl", compression="zip")
