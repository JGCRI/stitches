# Import packages
import fsspec
import pandas as pd
import stitches as stitches
import pkg_resources
import xarray as xr

# results will be written into a `tas_psl_pr` directory in the stitches package
# `data` directory. `tas_psl_pr` has `comparison` and `stitched` subdirectories
OUTPUT_DIR = pkg_resources.resource_filename('stitches', 'data')

# Stitch one realization per target ensemble, match in specified tolerance.
filter_future = True
Nmatches = 1
tolerance = 0.075
# #####################################################
# Helper functions
# #####################################################
# Function to remove any ensemble members from a target data frame that
# stop before 2099, for example, ending in 2014 like some MIROC6 SSP245:
def prep_target_data(target_df):
    if not target_df.empty:
        grped = target_df.groupby(['experiment', 'variable', 'ensemble', 'model'])
        for name, group in grped:
            df1 = group.copy()
            # if it isn't a complete time series (defined as going to 2099 or 2100),
            # remove it from the target data frame:
            if max(df1.end_yr) < 2099:
                target_df = target_df.loc[(target_df['ensemble'] != df1.ensemble.unique()[0])].copy().reset_index(
                    drop=True)
            del (df1)
        del (grped)

        target_df = target_df.reset_index(drop=True).copy()
        return(target_df)

# helper function to pull the GSAT data of the specific ensemble members in
# a target data frame:
def get_orig_GSAT_data(target_df):
    if not target_df.empty:
        esm_name = target_df.model.unique()[0]
        scn_name = target_df.experiment.unique()[0]

        full_rawtarget_path = pkg_resources.resource_filename('stitches', ('data/tas-data/' + esm_name + '_tas.csv'))
        full_rawtarget_data = pd.read_csv(full_rawtarget_path)

        orig_data = full_rawtarget_data[(full_rawtarget_data['experiment'] == scn_name)].copy()
        keys = ['experiment', 'ensemble', 'model']
        i1 = orig_data.set_index(keys).index
        i2 = target_df.set_index(keys).index
        orig_data = orig_data[i1.isin(i2)].copy()
        del (i1)
        del (i2)
        del (keys)
        del (full_rawtarget_data)
        del (full_rawtarget_path)

        orig_data = orig_data.reset_index(drop=True).copy()
        return (orig_data)


# ##########################################################
# We will do a run of a model with a lot of ensemble members,
# and then a model with few.
#
# First, we will pre-determine which ensembles have pr, psl, tas.
# The `make_recipe` function does this, but we'll do it up front for clarity.
# pangeo table of ESMs for reference
pangeo_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
pangeo_data = pd.read_csv(pangeo_path)
pangeo_data = pangeo_data[((pangeo_data['variable'] == 'tas') | (pangeo_data['variable'] == 'pr') | (pangeo_data['variable'] == 'psl'))
                          & ((pangeo_data['domain'] == 'Amon') ) ].copy()

# Keep only the runs that have data for all vars X all timesteps:
pangeo_good_ensembles =[]
for name, group in pangeo_data.groupby(['model', 'experiment', 'ensemble']):
    df = group.drop_duplicates().copy()
    if len(df) == 3:
        pangeo_good_ensembles.append(df)
    del(df)
pangeo_good_ensembles = pd.concat(pangeo_good_ensembles)
pangeo_good_ensembles  = pangeo_good_ensembles[['model', 'experiment', 'ensemble']].drop_duplicates().copy()
pangeo_good_ensembles = pangeo_good_ensembles.reset_index(drop=True).copy()

# won't use idealized runs
pangeo_good_ensembles = pangeo_good_ensembles[~((pangeo_good_ensembles['experiment'] == '1pctCO2') |
                                                (pangeo_good_ensembles['experiment'] == 'abrupt-4xCO2')|
                                                (pangeo_good_ensembles['experiment'] == 'ssp534-over')) ].reset_index(drop=True).copy()


# ##########################################################
# Load the archive data we want to match on.
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
data = pd.read_csv(archive_path)
data = data[data['experiment'].isin({'ssp126', 'ssp245', 'ssp370', 'ssp585',
                                         'ssp119',  'ssp534-over', 'ssp434', 'ssp460'})].copy()

# Keep only the entries that appeared in pangeo_good_ensembles:
keys =['model', 'experiment', 'ensemble']
i1 = data.set_index(keys).index
i2 = pangeo_good_ensembles.set_index(keys).index
data= data[i1.isin(i2)].copy()
del(i1)
del(i2)

# #####################################################
model_list = [ "CAMS-CSM1-0", "MIROC6"]

# model_list = ["ACCESS-CM2", "ACCESS-ESM1-5", "AWI-CM-1-1-MR", "CAMS-CSM1-0", "CanESM5", "CAS-ESM2-0",
#               "CESM2", "CESM2-WACCM", "FGOALS-g3", "FIO-ESM-2-0", "GISS-E2-1-G", "GISS-E2-1-H",
#               "HadGEM3-GC31-LL", "HadGEM3-GC31-MM", "MIROC-ES2L", "MIROC6", "MPI-ESM1-2-HR",
#               "MPI-ESM1-2-LR", "MRI-ESM2-0", "NorESM2-LM", "NorESM2-LM", "UKESM1-0-LL"]
for model_to_use in model_list:
    # Select the data to use as our archive.
    # For this paper experiment, we remain with the bracketing scenarios:
    archive_data = data[(data['model'] == model_to_use) &
                        ((data['experiment'] == 'ssp126') |
                         (data['experiment'] == 'ssp585'))].copy()

    # save off the ensembles in the archive for CT to check from if needed
    archive_data[['model', 'experiment', 'ensemble']].drop_duplicates().to_csv((OUTPUT_DIR + '/tas_psl_pr/archive_used_gridded_tas_psl_pr_' +
                 model_to_use + '.csv'), index=False)


    # Run for the ssp2-4.5 targets --------------------------------------------------------------
    # Select the some data to use as the target data
    target_data = data[data["model"] == model_to_use].copy()
    target_data = target_data[target_data["experiment"] == 'ssp245'].copy()
    target_data = target_data.reset_index(drop=True).copy()
    # remove any ensemble members that stop in 2014:
    target_data = prep_target_data(target_data).copy()

    # we will target  with the first 5 numerical ensemble members.
    # Not all models start the ensemble count at 1,
    # And not all experiments of a given model report the
    # same realizations.
    # select 5 ensemble realizations to
    # look at if there are more than 5.
    f = lambda x: x.ensemble[:x.idx]

    if not target_data.empty:
        ensemble_list = pd.DataFrame({'ensemble': target_data["ensemble"].unique()})
        ensemble_list['idx'] = ensemble_list['ensemble'].str.index('i')
        ensemble_list['ensemble_id'] = ensemble_list.apply(f, axis=1)
        ensemble_list['ensemble_id'] = ensemble_list['ensemble_id'].str[1:].astype(int)
        ensemble_list = ensemble_list.sort_values('ensemble_id').copy()
        if len(ensemble_list) > 5:
            ensemble_keep = ensemble_list.iloc[0:5].ensemble
        else:
            ensemble_keep = ensemble_list.ensemble

        target_data = target_data[target_data['ensemble'].isin(ensemble_keep)].copy()
        del (ensemble_keep)
        del (ensemble_list)

    # get original GSAT data corresponding to target
    orig_245 = get_orig_GSAT_data(target_data)

    # Make the multivariable recipe.
    rp = stitches.make_recipe(target_data=target_data,
                              archive_data=archive_data,
                              res="mon",
                              tol=tolerance,
                              non_tas_variables=["psl", "pr"],
                              N_matches=Nmatches,
                              reproducible=True)

    # Save the recipe
    rp.to_csv(OUTPUT_DIR + "/tas_psl_pr/" + model_to_use + "_ssp245_rp.csv",index=False)

    # Stitch save the stitched GSATs for quality assurance:
    gsat_245 = stitches.gmat_stitching(rp)
    gsat_245.to_csv((OUTPUT_DIR + '/tas_psl_pr/stitched_GSAT_data_ssp245_' +
             model_to_use + '.csv'), index=False)

    # stitch and  save the gridded data
    out = stitches.gridded_stitching((OUTPUT_DIR +"/tas_psl_pr/stitched"), rp)

    # save off info for the actual ensembles targeted so we can pull their real netcdfs
    target_245_info = target_data[['model','variable', 'experiment', 'ensemble']].drop_duplicates().copy()
    tmp = target_245_info[['model', 'variable', 'ensemble']].copy()
    tmp['experiment'] = 'historical'
    target_245_info = target_245_info.append(tmp).reset_index(drop=True).copy()
    del(tmp)


    # Run for the ssp3-7.0 targets --------------------------------------------------------------
    # Select the some data to use as the target data
    target_data = data[data["model"] == model_to_use].copy()
    target_data = target_data[target_data["experiment"] == 'ssp370'].copy()
    target_data = target_data.reset_index(drop=True).copy()
    # remove any ensemble members that stop in 2014:
    target_data = prep_target_data(target_data).copy()

    # we will target  with the first 5 numerical ensemble members.
    # Not all models start the ensemble count at 1,
    # And not all experiments of a given model report the
    # same realizations.
    # select 5 ensemble realizations to
    # look at if there are more than 5.
    f = lambda x: x.ensemble[:x.idx]

    if not target_data.empty:
        ensemble_list = pd.DataFrame({'ensemble': target_data["ensemble"].unique()})
        ensemble_list['idx'] = ensemble_list['ensemble'].str.index('i')
        ensemble_list['ensemble_id'] = ensemble_list.apply(f, axis=1)
        ensemble_list['ensemble_id'] = ensemble_list['ensemble_id'].str[1:].astype(int)
        ensemble_list = ensemble_list.sort_values('ensemble_id').copy()
        if len(ensemble_list) > 5:
            ensemble_keep = ensemble_list.iloc[0:5].ensemble
        else:
            ensemble_keep = ensemble_list.ensemble

        target_data = target_data[target_data['ensemble'].isin(ensemble_keep)].copy()
        del (ensemble_keep)
        del (ensemble_list)

    # get original GSAT data corresponding to target
    orig_370= get_orig_GSAT_data(target_data)

    # Make the multivariable recipe.
    rp = stitches.make_recipe(target_data=target_data,
                              archive_data=archive_data,
                              res="mon",
                              tol=tolerance,
                              non_tas_variables=["psl", "pr"],
                              N_matches=Nmatches,
                              reproducible=True)

    # Save the recipe
    rp.to_csv(OUTPUT_DIR + "/tas_psl_pr/" + model_to_use + "_ssp370_rp.csv", index=False)

    # Stitch save the stitched GSATs for quality assurance:
    gsat_370 = stitches.gmat_stitching(rp)
    gsat_370.to_csv((OUTPUT_DIR + '/tas_psl_pr/stitched_GSAT_data_ssp370_' +
                     model_to_use + '.csv'), index=False)

    # Stitch and save the gridded data:
    out = stitches.gridded_stitching((OUTPUT_DIR + "/tas_psl_pr/stitched"), rp)

    # save off info for the actual ensembles targeted so we can pull their real netcdfs
    target_370_info = target_data[['model', 'variable', 'experiment', 'ensemble']].drop_duplicates().copy()
    tmp = target_370_info[['model', 'variable', 'ensemble']].copy()
    tmp['experiment'] = 'historical'
    target_370_info = target_370_info.append(tmp).reset_index(drop=True).copy()
    del (tmp)


    # Save a copy for comparison data --------------------------------------------------------------
    # Get a copy off the pangeo table
    pangeo_table = stitches.fx_pangeo.fetch_pangeo_table()
    pangeo_table = pangeo_table[(pangeo_table["source_id"] == model_to_use) &
                                (pangeo_table["experiment_id"].isin(['ssp245', 'ssp370', 'historical'])) &
                                (pangeo_table["table_id"].str.contains("mon")) &
                                (pangeo_table["variable_id"].isin(["tas", "psl", "pr"]))].copy().reset_index(drop=True)

    # further subset to just the ensemble members in our targets:
    keep = target_245_info.append(target_370_info).drop_duplicates().reset_index(drop=True)[['model', 'experiment', 'ensemble']]
    # rename the columns of keep to be consistent with the pangeo_table:
    keep = keep.rename(columns = {'model':'source_id',
                                  'experiment': 'experiment_id',
                                  'ensemble' :'member_id'}).copy()
    # save it off; These are the actual ensembles targeted, which is not necessarily
    # the same as ensembles stitched (since we can run out of viable matches):
    keep[keep['experiment_id'] != 'historical'].to_csv((OUTPUT_DIR + '/tas_psl_pr/ensembles_targeted_gridded_tas_psl_pr_' +
                                      model_to_use+ '.csv'), index=False)

    # and use keep to filter the pangeo_table
    keys = list(keep.columns.values)
    i1 = pangeo_table.set_index(keys).index
    i2  = keep.set_index(keys).index
    pangeo_table = pangeo_table[i1.isin(i2)].reset_index(drop=True).copy()

    # Save a copy of the data.
    for f in pangeo_table["zstore"]:
        ds = xr.open_zarr(fsspec.get_mapper(f))
        ds.sortby('time')

        info = pangeo_table[pangeo_table["zstore"] == f].copy().reset_index(drop=True)
        fname = (OUTPUT_DIR +"/tas_psl_pr/comparison/comparison_" + info.experiment_id[0] + "_" +
                 info.source_id[0] + "_" + info.member_id[0] + "_" +
                 info.variable_id[0] + ".nc")
        ds.to_netcdf(fname)

    # Save a copy of the GSAT data as well:
    orig_245.append(orig_370).to_csv((OUTPUT_DIR + '/tas_psl_pr/comparison_GSAT_data_' +
                                      model_to_use+ '.csv'), index=False)

# end for loop over ESMs


# Filter generated files to future if needed
if filter_future:
    from pathlib import Path
    # read in each stitched realization, subset to 2015
    entries = Path((OUTPUT_DIR + '/tas_psl_pr/stitched/'))
    for entry in entries.iterdir():
        if ('.nc' in entry.name) :
            print(entry.name)
            ds = xr.open_dataset((OUTPUT_DIR + '/tas_psl_pr/stitched/') + entry.name)
            ds = ds.sel(time = slice('2015-01-31', '2100-12-31')).copy()
            ds.to_netcdf((OUTPUT_DIR + '/tas_psl_pr/stitched_future/') + entry.name)
            del(ds)




