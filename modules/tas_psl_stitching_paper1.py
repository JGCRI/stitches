# Import packages
import fsspec
import pandas as pd
import stitches as stitches
import pkg_resources
import xarray as xr


# Load the archive data we want to match on.
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
data = pd.read_csv(archive_path)
data = data[data['experiment'].isin({'ssp126', 'ssp245', 'ssp370', 'ssp585',
                                         'ssp119',  'ssp534-over', 'ssp434', 'ssp460'})].copy()

model_list = ["ACCESS-CM2", "ACCESS-ESM1-5", "AWI-CM-1-1-MR", "CAMS-CSM1-0", "CanESM5", "CAS-ESM2-0",
              "CESM2", "CESM2-WACCM", "FGOALS-g3", "FIO-ESM-2-0", "GISS-E2-1-G", "GISS-E2-1-H",
              "HadGEM3-GC31-LL", "HadGEM3-GC31-MM", "MIROC-ES2L", "MIROC6", "MPI-ESM1-2-HR",
              "MPI-ESM1-2-LR", "MRI-ESM2-0", "NorESM2-LM", "NorESM2-LM", "UKESM1-0-LL"]

for model_to_use in model_list:

     # Run for the ssp2-4.5 targets --------------------------------------------------------------
     # Select the some data to use as the target data
     target_data = data[data["model"] == model_to_use]
     target_data = target_data[target_data["experiment"] == 'ssp245']
     target_data = target_data.reset_index(drop=True)

     # Select the data to use as our archive.
     archive_data = data[(data['model'] == model_to_use) & (data['experiment'] != 'ssp245')].copy()

     # Here is an example of how to make a recipe using the make recipe function, is basically wraps
     # all of the matching & formatting steps into a single function. Res can be set to "mon" or "day"
     # to indicate the resolution of the stitched data. Right now day & res are both working!
     # However we may run into issues with different types of calendars.
     rp = stitches.make_recipe(target_data=target_data, archive_data=archive_data, res="mon", tol=0.1,
                               non_tas_variables=["psl", "pr"], N_matches=100)
     rp.to_csv("./tas_psl_pr/" + model_to_use + "_ssp245_rp.csv")
     out = stitches.gridded_stitching("./tas_psl_pr/stitched", rp)

     # Run for the ssp3-7.0 targets --------------------------------------------------------------
     # Select the some data to use as the target data
     target_data = data[data["model"] == model_to_use]
     target_data = target_data[target_data["experiment"] == 'ssp370']
     target_data = target_data.reset_index(drop=True)

     # Select the data to use as our archive.
     archive_data = data[(data['model'] == model_to_use) & (data['experiment'] != 'ssp370')].copy()

     # Here is an example of how to make a recipe using the make recipe function, is basically wraps
     # all of the matching & formatting steps into a single function. Res can be set to "mon" or "day"
     # to indicate the resolution of the stitched data. Right now day & res are both working!
     # However we may run into issues with different types of calendars.
     rp = stitches.make_recipe(target_data=target_data, archive_data=archive_data, res="mon", tol=0.1,
                               non_tas_variables=["psl", "pr"], N_matches=100)
     rp.to_csv("./tas_psl_pr/" + model_to_use + "_ssp370_rp.csv")
     out = stitches.gridded_stitching("./tas_psl_pr/stitched", rp)

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
        fname = ("./tas_psl_pr/comparison/comparison_" + info.experiment_id[0] + "_" +
                 info.source_id[0] + "_" + info.member_id[0] + "_" +
                 info.variable_id[0] + ".nc")
        ds.to_netcdf(fname)

