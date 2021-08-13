# Save a copy of the pangeo table, this has information about the z store file.
# #############################################################################
# General setup
# #############################################################################
# Import packages
import stitches.fx_pangeo as pangeo
import pickle_utils as pickle

#############################################################################################
# 1. the pangeo meta data
#############################################################################################
# Get the pangeo table of contents.
df = pangeo.fetch_pangeo_table()

# Subset the monthly tas data, these are the files that we will want to process
# for the tas archive.
xps = ["historical", "1pctCO2", "abrupt-4xCO2", "abrupt-2xCO2", "ssp370", "ssp245", "ssp119",
       "ssp434", "ssp460", "ssp126", "ssp585", "ssp534-over"]
df = df[df["experiment_id"].isin(xps)]  # experiments of interest
df = df[df["grid_label"] == "gn"]   # we are only interested in the results returned in the native grid

pangeo_df = df[["source_id", "experiment_id", "member_id", "variable_id", "zstore", "table_id"]].drop_duplicates()
pangeo_df = pangeo_df.rename(columns={"source_id": "model", "experiment_id": "experiment",
                                      "member_id": "ensemble", "variable_id": "variable",
                                      "table_id": "res"})

#############################################################################################
# 2. Save
#############################################################################################
# Save a copy of the contents of the pangeo data frame, these are the values
# that will be used to fetch the netcdf files from pangeo.
# Note that this file has to be compressed so will need to read in
# using pickle_utils.load()
pickle.dump(pangeo_df, "stitches/data/pangeo_table.pkl", compression="zip")

