# Not a notebook since community version of pycharm does
# not offer jupyter lab integration.

# Eventually, this needs to live in the create_archive_library module
# and its outputs need to be minted in zenodo and brought in via
# install supplement https://github.com/IMMM-SFA/im3py/blob/master/im3py/install_supplement.py
# for use in the do_emulation module

# * Names subject to change.

# TODO update the way we handle where things are getting saved

###############################################################################
# Load the actual package and shared libraries
###############################################################################

from stitches.pkgimports import *

import stitches.readpangeo as read
import stitches.calculatetgav as tgav


###############################################################################
# Open up the connection to pangeo
###############################################################################

# open up the full pangeo set of data
pangeo = intake.open_esm_datastore("https://storage.googleapis.com/cmip6/pangeo-cmip6.json")


###############################################################################
# Get a first cut list of models for proof of concept.
# Only take models that have daily data; if they have daily, they almost
# certainly have monthly. Daily netcdfs are less commonly submitted, so doing
# the search on only daily will probably return fewer models for our proof of
# concept + then we # can validate on both monthly and daily data.
###############################################################################

# define experiments of interest
expts = ['ssp126', 'ssp245', 'ssp370', 'ssp585', 'ssp119', 'ssp434',
         'ssp460', 'ssp534-over', "historical"]

table_ids = ['day']

count_table = read.create_preliminary_model_list(pangeo,
                                                 experiments=expts,
                                                 table_ids=table_ids,
                                                 min_ensemble_size=5)

# Print the full list of models to process
print('Models to process')
print(count_table["source_id"].values)


###############################################################################
# Turn the list of experiments and the models in count_table into a query to
# get a pangeo subsetted list of files that we want to loop over.
###############################################################################

# CMIP6 models don't submit annual netcdfs, so we will calculate
# tgav from monthly data.
query = dict(
    experiment_id=expts,
    variable_id='tas',
    source_id=count_table["source_id"].copy(),
    table_id='Amon',
    grid_label='gn'
)

# Subset the pangeo catalog with our query.
# This is the master list of files that we want to calculate Tgav across.
# Subset further to only keep the p1 physics setting from each model.
pangeo_subset = pangeo.search(**query)

pangeo_subset.df = read.keep_p1_results(pangeo_subset).copy()


###############################################################################
# Summarize how many files we have to process when all is said and done:
###############################################################################

# pangeo_subset.df is now our master list of files to calculate Tgav for each.

# Find how many ensemble members each model did for each experiment
print('Number of files to process:')
print(pangeo_subset.df.groupby("source_id")[["activity_id", "experiment_id", "member_id"]].nunique())
print('---------------------------------------------------------------')
print('Total Number files:')
print(pangeo_subset.df['source_id'].count())

###############################################################################
# Load and process all of these.
# Doing it in serial - it will take a long time but I can do it with what I know
# now and it will work.
###############################################################################

# we're going to save this as a master csv file instead of netcdfs because we
# just don't need that structure and the associated memory footprint.
pangeo_df = pangeo_subset.df.copy()
pangeo_df = pangeo_df[["activity_id", "source_id", "experiment_id",
                       "member_id", "table_id", "variable_id",
                       "grid_label", "zstore"]]

# save the pangeo list metadata
pangeo_df.to_csv("stitches/data/created_data/pangeo_file_list_for_tgav.csv")
# are some cases where pickle-ing [to_pickle()] lets you save
# more structured files for ingesting back in later so that it
# comes back in as the exact data structure (like saving as RDS)

# Initialize empty holder
all_tgav = pd.DataFrame()

for file_index in list(range(0, len(pangeo_df))):
    tgav.calc_and_format_tgav(pangeo_df.iloc[file_index], save_individ_tgav=True)

    # and append to the full list
    all_tgav = all_tgav.append(tgav.calc_and_format_tgav(pangeo_df.iloc[file_index], save_individ_tgav=True))
# end for loop over file index


# save the full file of tgav values
all_tgav.to_csv('stitches/data/created_data/main_tgav_all_pangeo_list_models.csv', index=False)