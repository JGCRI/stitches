#  thought for the paper and the high frequency/other variables validation we
#  could limit ourselves (at least until we see what happens with this first
#  exploration) to look at one stitched output for a model for which the
#  emulation is very good, and one for a model for which the emulation shows
#  big jumps. For these two cases I would like to get all the gridded output
#  (but monthly only) TAS/PR and SLP.

#  I think I would need both emulated and real stuff to compare. In the case of
#  the bad one we only have one stitched result; in the case of the good one
#  we have two, and either would do.


# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import numpy as np
import stitches as stitches
import pkg_resources
import os
from  pathlib import Path

# pd.set_option('display.max_columns', None)

# OUTPUT_DIR = pkg_resources.resource_filename('stitches', 'data/created_data')
OUTPUT_DIR = '/pic/projects/GCAM/stitches_pic/paper1_outputs'

# #############################################################################
# Experiment  setup
# #############################################################################
# experiment parameters
tolerances = [0.075] # np.round(np.arange(0.07, 0.225, 0.005), 3)
Ndraws = 1
error_threshold = 0.1

# pangeo table of ESMs for reference
pangeo_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
pangeo_data = pd.read_csv(pangeo_path)
pangeo_data = pangeo_data[((pangeo_data['variable'] == 'tas') | (pangeo_data['variable'] == 'pr') | (pangeo_data['variable'] == 'psl'))
                          & ((pangeo_data['domain'] == 'Amon') | (pangeo_data['domain'] == 'day')) ].copy()

# Keep only the runs that have data for all vars X all timesteps:
pangeo_good_ensembles =[]
for name, group in pangeo_data.groupby(['model', 'experiment', 'ensemble']):
    df = group.drop_duplicates().copy()
    if len(df) == 6:
        pangeo_good_ensembles.append(df)
    del(df)
pangeo_good_ensembles = pd.concat(pangeo_good_ensembles)
pangeo_good_ensembles  = pangeo_good_ensembles[['model', 'experiment', 'ensemble']].drop_duplicates().copy()
pangeo_good_ensembles = pangeo_good_ensembles.reset_index(drop=True).copy()

# won't use idealized runs
pangeo_good_ensembles = pangeo_good_ensembles[~((pangeo_good_ensembles['experiment'] == '1pctCO2') |
                                                (pangeo_good_ensembles['experiment'] == 'abrupt-4xCO2')) ].reset_index(drop=True).copy()

esms = ['CMCC-CM2-SR5', 'NorESM2-MM']
# ['ACCESS-CM2', 'ACCESS-ESM1-5', 'AWI-CM-1-1-MR', 'BCC-CSM2-MR',
#        'BCC-ESM1', 'CESM2', 'CESM2-FV2', 'CESM2-WACCM', 'CMCC-CM2-HR4',
#        'CMCC-CM2-SR5', 'CMCC-ESM2', 'CanESM5', 'HadGEM3-GC31-LL',
#        'HadGEM3-GC31-MM', 'IITM-ESM', 'MIROC-ES2L', 'MIROC6',
#        'MPI-ESM-1-2-HAM', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0',
#        'NorESM2-LM', 'NorESM2-MM', 'SAM0-UNICON', 'TaiESM1',
#        'UKESM1-0-LL']

# #############################################################################
# Load full archive and target data
# #############################################################################

# Load the full archive of all staggered windows, which we will be matching on
full_archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_archive_data = pd.read_csv(full_archive_path)

# Keep only the entries that appeared in pangeo_good_ensembles:
keys =['model', 'experiment', 'ensemble']
i1 = full_archive_data.set_index(keys).index
i2 = pangeo_good_ensembles.set_index(keys).index
full_archive_data= full_archive_data[i1.isin(i2)].copy()
del(i1)
del(i2)

# Load the original archive without staggered windows, which we will draw
# the target trajectories from for matching
full_target_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_target_data = pd.read_csv(full_target_path)

# Keep only the entries that appeared in pangeo_good_ensembles:
keys =['model', 'experiment', 'ensemble']
i1 = full_target_data.set_index(keys).index
i2 = pangeo_good_ensembles.set_index(keys).index
full_target_data = full_target_data[i1.isin(i2)].copy()
del(i1)
del(i2)
del(keys)

# #############################################################################
# Some helper functions
# #############################################################################
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


def get_orig_data(target_df):
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

def get_orig_netcdfs(target_df, res, non_tas_vars, pt, DIR):

    mod = target_df['model'].unique()[0]
    exper =  target_df['experiment'].unique()[0]
    ens = target_df['ensemble'].unique()

    if res=='mon':
        res = 'Amon'

    p = pt[(pt['model'] == mod) & (pt['experiment'] == exper) & (pt['domain'] == res)].copy()
    p = p[p['ensemble'].isin(ens)].copy()

    p_hist =  pt[(pt['model'] == mod) & (pt['experiment'] == 'historical') & (pt['domain'] == res)].copy()
    p_hist = p_hist[p_hist['ensemble'].isin(ens)].copy()

    p = p.append(p_hist).reset_index(drop=True).copy()

    non_tas_vars.append('tas')

    for v in non_tas_vars:

        filelist = p[p['variable']==v].zstore

        for f in filelist:
           ds = stitches.fx_pangeo.fetch_nc(f)
           f1 = p[(p['variable']==v) & (p['zstore'] == f)].reset_index(drop=True).copy()
           ds.to_netcdf((DIR + "/" + v + "_" + res + "_" + mod + "_" + str(f1.experiment[0]) + "_" + str(f1.ensemble[0]) + "_orig.nc"))

    # end get orig_netcdfs fcn

def get_jumps(tgav_df):
    tgav_jump = []
    for name, group in tgav_df.groupby(['variable', 'experiment', 'ensemble', 'model']):
        ds = group.copy()
        ds['jump'] = ds.value.diff().copy()
        ds = ds.dropna().copy()
        tgav_jump.append(ds)
        del (ds)
    tgav_jump = pd.concat(tgav_jump)
    tgav_jump = tgav_jump.drop(columns=['value']).copy()
    tgav_jump = tgav_jump.drop_duplicates().reset_index(drop=True).copy()
    return(tgav_jump)

def four_errors(gen_data, orig_data):
    gen_data_jump = get_jumps(gen_data)
    orig_data_jump = get_jumps(orig_data)

    orig_stats = []
    for name, group in orig_data.groupby(['model', 'variable', 'experiment']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment']].drop_duplicates().copy()
        ds1['mean_orig_tgav'] = np.mean(ds.value.values)
        ds1['sd_orig_tgav'] = np.std(ds.value.values)
        orig_stats.append(ds1)
        del (ds)
        del (ds1)
    orig_stats = pd.concat(orig_stats).reset_index(drop=True).copy()

    orig_stats_jump = []
    for name, group in orig_data_jump.groupby(['model', 'variable', 'experiment']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment']].drop_duplicates().copy()
        ds1['mean_orig_jump'] = np.mean(ds.jump.values)
        ds1['sd_orig_jump'] = np.std(ds.jump.values)
        orig_stats_jump.append(ds1)
        del (ds)
        del (ds1)
    orig_stats_jump = pd.concat(orig_stats_jump).reset_index(drop=True).copy()

    orig_stats = orig_stats.merge(orig_stats_jump, how='left', on=['model', 'variable', 'experiment']).copy()
    del (orig_stats_jump)

    gen_stats = []
    for name, group in gen_data.groupby(['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']].drop_duplicates().copy()
        ds1['mean_gen_tgav'] = np.mean(ds.value.values)
        ds1['sd_gen_tgav'] = np.std(ds.value.values)
        gen_stats.append(ds1)
        del (ds)
        del (ds1)
    gen_stats = pd.concat(gen_stats).reset_index(drop=True).copy()

    gen_stats_jump = []
    for name, group in gen_data_jump.groupby(['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']].drop_duplicates().copy()
        ds1['mean_gen_jump'] = np.mean(ds.jump.values)
        ds1['sd_gen_jump'] = np.std(ds.jump.values)
        gen_stats_jump.append(ds1)
        del (ds)
        del (ds1)
    gen_stats_jump = pd.concat(gen_stats_jump).reset_index(drop=True).copy()

    gen_stats = gen_stats.merge(gen_stats_jump, how='left',
                                on=['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']).copy()
    del (gen_stats_jump)

    compare = gen_stats.merge(orig_stats, how='left', on=['model', 'variable', 'experiment']).copy()
    del (gen_stats)
    del (orig_stats)

    compare['E1_tgav'] = abs(compare.mean_orig_tgav - compare.mean_gen_tgav) / compare.sd_orig_tgav
    compare['E2_tgav'] = compare.sd_gen_tgav / compare.sd_orig_tgav

    compare['E1_jump'] = abs(compare.mean_orig_jump - compare.mean_gen_jump) / compare.sd_orig_jump
    compare['E2_jump'] = compare.sd_gen_jump / compare.sd_orig_jump

    compare = compare[['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive',
                       'E1_tgav', 'E2_tgav', 'E1_jump', 'E2_jump']].copy()

    four_values = []
    for name, group in compare.groupby(['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']):
        ds = group.copy()
        ds['max_metric'] = np.max(
            [ds.E1_tgav.values, abs(1 - ds.E2_tgav.values), ds.E1_jump.values, abs(1 - ds.E2_jump.values)])
        four_values.append(ds)
        del (ds)
    four_values = pd.concat(four_values).reset_index(drop=True).copy()
    del (compare)

    return(four_values)


# #############################################################################
# The experiment
# #############################################################################

# for each of the esms in the experiment, subset to what we want
# to work with and run the experiment.
for esm in esms:
    print(esm)

    # subset the archive and the targets to this ESM
    archive_w_all = full_archive_data[(full_archive_data['model'] == esm)].copy()


    target_245 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp245')].copy()

    target_370 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp370')].copy()


    # Clean up target data and pull corresponding original/raw data
    if not target_245.empty:
        # clean up
        target_245 = prep_target_data(target_245).copy()

        # and pull corresponding original/raw data for later comparison
        orig_245 = get_orig_data(target_245).copy()
        get_orig_netcdfs(target_df=target_245,
                         res='mon',
                         non_tas_vars=['pr', 'psl'],
                         pt=pangeo_data,
                         DIR=(OUTPUT_DIR + "/" + esm + "/high_freq"))


    if not target_370.empty:
        # clean up
        target_370 = prep_target_data(target_370).copy()

        # and pull corresponding original/raw data for later comparison
        orig_370 = get_orig_data(target_370).copy()
        get_orig_netcdfs(target_df=target_370,
                         res='mon',
                         non_tas_vars=['pr', 'psl'],
                         pt=pangeo_data,
                         DIR=(OUTPUT_DIR + "/" + esm + "/high_freq"))



    # loop over tolerances:
    for tolerance in tolerances:
        rp_245 = stitches.make_recipe(target_data=target_245,
                                      archive_data=archive_w_all,
                                      N_matches=20000,
                                      res="mon",
                                      tol=tolerance,
                                      non_tas_variables=["pr", "psl"])
        rp_245.to_csv((OUTPUT_DIR + "/" + esm + "/high_freq/recipes_for_target_" + esm + '_ssp245.csv'), index=False)
        out_245 = stitches.gridded_stitching((OUTPUT_DIR + "/" + esm + "/high_freq"), rp_245)

        rp_370 = stitches.make_recipe(target_data=target_370,
                                      archive_data=archive_w_all,
                                      N_matches=20000,
                                      res="mon",
                                      tol=tolerance,
                                      non_tas_variables=["pr", "psl"])
        rp_370.to_csv((OUTPUT_DIR + "/" + esm + "/high_freq/recipes_for_target_" + esm +'_ssp370.csv' ), index=False)
        out_370 = stitches.gridded_stitching((OUTPUT_DIR + "/" + esm + "/high_freq"), rp_370)



# end for loop over ESMs
