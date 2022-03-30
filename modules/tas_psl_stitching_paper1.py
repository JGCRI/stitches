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
Nmatches = 1
tolerance = 0.075



# Load the archive data we want to match on.
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
data = pd.read_csv(archive_path)
data = data[data['experiment'].isin({'ssp126', 'ssp245', 'ssp370', 'ssp585',
                                         'ssp119',  'ssp534-over', 'ssp434', 'ssp460'})].copy()

# ##########################################################
# We will do a run of a model with a lot of ensemble members,
# and then a model with few.
#
# # Code for determining models
# pangeo_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
# pangeo_data = pd.read_csv(pangeo_path)
# pangeo_data = pangeo_data[((pangeo_data['variable'] == 'tas') | (pangeo_data['variable'] == 'pr') | (pangeo_data['variable'] == 'psl'))
#                           & ((pangeo_data['domain'] == 'Amon') ) ].copy()
#
# # Keep only the runs that have data for all vars X all timesteps:
# pangeo_good_ensembles =[]
# for name, group in pangeo_data.groupby(['model', 'experiment', 'ensemble']):
#     df = group.drop_duplicates().copy()
#     if len(df) == 3:
#         pangeo_good_ensembles.append(df)
#     del(df)
# pangeo_good_ensembles = pd.concat(pangeo_good_ensembles)
# pangeo_good_ensembles  = pangeo_good_ensembles[['model', 'experiment', 'ensemble']].drop_duplicates().copy()
# pangeo_good_ensembles = pangeo_good_ensembles.reset_index(drop=True).copy()
#
# x = pangeo_good_ensembles.drop_duplicates().reset_index(drop=True).copy()
# x = x[(x['experiment'] == 'ssp126') | (x['experiment'] == 'ssp585')].copy()
# holder = []
# grped = x.groupby(['model', 'experiment'])
# for name, group in grped:
#     df1 = group.drop_duplicates().copy()
#     df1['n_ens'] = len(df1)
#     holder.append(df1[['model', 'experiment', 'n_ens']].drop_duplicates())
# holder = pd.concat(holder).drop_duplicates().reset_index(drop=True).copy()

model_list = ["CAMS-CSM1-0", "MIROC6"]
# ##########################################################
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


    # Run for the ssp2-4.5 targets --------------------------------------------------------------
    # Select the some data to use as the target data
    target_data = data[data["model"] == model_to_use].copy()
    target_data = target_data[target_data["experiment"] == 'ssp245'].copy()
    target_data = target_data.reset_index(drop=True).copy()


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


    # Here is an example of how to make a recipe using the make recipe function, is basically wraps
    # all of the matching & formatting steps into a single function. Res can be set to "mon" or "day"
    # to indicate the resolution of the stitched data. Right now day & res are both working!
    # However we may run into issues with different types of calendars.
    rp = stitches.make_recipe(target_data=target_data,
                              archive_data=archive_data,
                              res="mon",
                              tol=tolerance,
                              non_tas_variables=["psl", "pr"],
                              N_matches=Nmatches,
                              reproducible=True)
    rp.to_csv(OUTPUT_DIR + "/tas_psl_pr/" + model_to_use + "_ssp245_rp.csv")
    out = stitches.gridded_stitching((OUTPUT_DIR +"/tas_psl_pr/stitched"), rp)

    # Run for the ssp3-7.0 targets --------------------------------------------------------------
    # Select the some data to use as the target data
    target_data = data[data["model"] == model_to_use].copy()
    target_data = target_data[target_data["experiment"] == 'ssp370'].copy()
    target_data = target_data.reset_index(drop=True).copy()

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

    # Here is an example of how to make a recipe using the make recipe function, is basically wraps
    # all of the matching & formatting steps into a single function. Res can be set to "mon" or "day"
    # to indicate the resolution of the stitched data. Right now day & res are both working!
    # However we may run into issues with different types of calendars.
    rp = stitches.make_recipe(target_data=target_data,
                              archive_data=archive_data,
                              res="mon",
                              tol=tolerance,
                              non_tas_variables=["psl", "pr"],
                              N_matches=Nmatches,
                              reproducible=True)
    rp.to_csv(OUTPUT_DIR + "/tas_psl_pr/" + model_to_use + "_ssp370_rp.csv")
    out = stitches.gridded_stitching((OUTPUT_DIR + "/tas_psl_pr/stitched"), rp)


    # Save a copy for comparison data --------------------------------------------------------------
    # Get a copy off the pangeo table
    pangeo_table = stitches.fx_pangeo.fetch_pangeo_table()
    pangeo_table = pangeo_table[(pangeo_table["source_id"] == model_to_use) &
                                (pangeo_table["experiment_id"].isin(['ssp245', 'ssp370', 'historical'])) &
                                (pangeo_table["table_id"].str.contains("mon")) &
                                (pangeo_table["variable_id"].isin(["tas", "psl", "pr"]))].copy().reset_index(drop=True)

    # Save a copy of the data.
    for f in pangeo_table["zstore"]:
        ds = xr.open_zarr(fsspec.get_mapper(f))
        ds.sortby('time')

        info = pangeo_table[pangeo_table["zstore"] == f].copy().reset_index(drop=True)
        fname = (OUTPUT_DIR +"/tas_psl_pr/comparison/comparison_" + info.experiment_id[0] + "_" +
                 info.source_id[0] + "_" + info.member_id[0] + "_" +
                 info.variable_id[0] + ".nc")
        ds.to_netcdf(fname)

# end for loop over ESMs
