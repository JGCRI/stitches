# #############################################################################
## General setup
# #############################################################################
from stitches.pkgimports import *

# Load the matchup related functions.
import stitches.dev_matchup as matchup

# plotting aesthetic
sns.set_style('whitegrid')

# have pandas print all columns in console
pd.set_option('display.max_columns', None)


# #############################################################################
## Import data and select single model to work with
# #############################################################################
# Import the data and select the model to use, I suspect that in the future these will be
# combined into a single function call.
data = matchup.cleanup_main_tgav("./stitches/data/created_data/main_tgav_all_pangeo_list_models.csv")

# Select an ESM to work with from
# ['CanESM5', 'ACCESS-ESM1-5', 'MIROC6', 'UKESM1-0-LL',
#  'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'GISS-E2-1-G', 'NorCPM1']
ESM = "GISS-E2-1-G"
tgav_data = matchup.select_model_to_emulate(ESM, data)


# #############################################################################
## some summary and cleanup
# #############################################################################
# investigate how many ensemble members per experiment
a = tgav_data[['experiment', 'ensemble']].drop_duplicates().copy()
print(a.groupby('experiment').agg(['count']))

# Calculate the temperature anomaly relative to 1995 - 2014 (IPCC reference period).
t_anomaly = matchup.calculate_anomaly(tgav_data).drop('activity', 1)

# So it turns out that something funky is going on with the ssp534-over values, it looks like
# the time series is incomplete for some reason, not really sure what we want to do with that
# but for now let's just remove it.
t_anomaly = t_anomaly[t_anomaly["experiment"] != "ssp534-over"]

p0 = sns.relplot(data=t_anomaly, x='year', y='value',
                 hue='ensemble',
                 col='experiment',
                 kind='line',
                 linewidth=0.6)
p0.fig.subplots_adjust(top=0.85)
p0.fig.suptitle(ESM + " all ensemble members, raw data")
p0.savefig('./figs/' + ESM + '/' + ESM + '_time_series_all_ensemble_raw_data.png')

# #############################################################################
## Calculate and plot the reference (ensemble avg) for each scenario
# #############################################################################
# Calculate reference anomaly Tgav time series for each experiment = the
# average across ensemble members for each experiment.
# want the grouping to be over experiment and year (so that we take the average
# for each of those), but we group by  model
t_anom_ref = t_anomaly.groupby(['model', 'experiment', 'year',
                                'timestep', 'grid_type','variable']).agg(
        {'value': lambda x: sum(x) / len(x)}).reset_index().rename(columns={'value':'refvalue'}).copy()


# plot it
# seaborn is very similar to using ggplot but it is _so slow_
#p = sns.FacetGrid(data=t_anom_ref, col='experiment')# setting it up with seaborn
#p.map(plt.scatter, x=t_anom_ref['year'], y=t_anom_ref['value'])# doing the plot
## TODO: make this a nice plotting helper so we have standardized figures
## TODO: figure out aspect ratio stuff in seaborn.
p1 = sns.relplot(data=t_anom_ref, x='year', y='refvalue',
                 col='experiment', kind='line')
p1.fig.subplots_adjust(top=0.85)
p1.fig.suptitle(ESM + " reference Tgav anomalies - smoothed across ensemble members")
p1.set_axis_labels('Year', 'Tgav anomaly from 1995-2014, degC')
p1.savefig('./figs/' + ESM + '/' + ESM + '_ensemble_avg_byScenario.png')


# #############################################################################
## Loop over all window sizes under consideration, smooth each ensemble member,
## compare to each scenario reference, save the RMSE comparison.
# #############################################################################
window_rms = pd.DataFrame()
window_rms_by_scenario = pd.DataFrame()
# loop over window size, smooth the raw data, compare to the ref value via rms, output something
for windowsize in list(range(1, 36)):

    # note that matchup.calculate_rolling_mean appends the historical 1950-2014 data to
    # the projected 2015-2100 data for each SSP-RCP scenario for smoothing. So Once we
    # smooth everything, we have to relable the historical years as historical to compare
    # to the ensemble-averaged reference.
    smoothed_t_anom = matchup.calculate_rolling_mean(t_anomaly, windowsize)

    # relabel hitorical years as historical,
    smoothed_t_anom.loc[smoothed_t_anom['year'] < 2015, 'experiment'] = 'historical'

    # join in the reference values
    t_anom = smoothed_t_anom.merge(t_anom_ref,
                                   on=['timestep', 'grid_type',
                                       'model', 'experiment', 'year', 'variable'],
                                   how='left').copy()

    # get the deviations from reference for each value
    t_anom['diff'] = t_anom['value'] - t_anom['refvalue']

    # group by ensemble to get an rms summary
    ens_rms = t_anom.groupby(['model', 'experiment', 'ensemble', 'variable',
                              'timestep', 'grid_type']).agg(
        {'diff': lambda x: np.sqrt(np.nansum(x*x) / len(x))}).reset_index().rename(
        columns={'diff':'rms'}).copy()

    # so that gives us the average distance between each ESM ensemble run and the reference
    # scenario (for each SSP-RCP).

    # Define 'smooth enough' as when the ensemble in the SSP-RCP scenario with the greatest
    # distance from reference is less than tolerance.

    # Get that max distance within each scenario:
    ens_rms_scen = ens_rms.groupby(['model', 'variable',  'experiment',
                               'timestep', 'grid_type']).agg(
        {'rms': lambda x: max(x)}).reset_index().rename(
        columns={'rms': 'maxrms'}).copy()

    ens_rms_scen['window'] = windowsize

    # save out that max distance info.
    window_rms_by_scenario = window_rms_by_scenario.append(ens_rms_scen)


    # Get that max distance across all scenarios:
    ens_rms = ens_rms.groupby(['model', 'variable',
                              'timestep', 'grid_type']).agg(
        {'rms': lambda x: max(x)}).reset_index().rename(
        columns={'rms':'maxrms'}).copy()

    ens_rms['window'] = windowsize

    # save out that max distance info.
    window_rms = window_rms.append(ens_rms)

# end for loop

# #############################################################################
## plot the errors for each window size
# #############################################################################
# plot that RMS across all scenarios
p3 = sns.relplot(data=window_rms, x='window', y='maxrms', kind='scatter',
                 hue='window')
p3.fig.subplots_adjust(top=0.85)
p3.fig.suptitle(ESM + " window size vs max RMSE by ensemble member and scenario")

p3.savefig('./figs/' + ESM + '/' + ESM + '_RMSvsWindow_allScenarios.png')

# plot for each scenario
p2 = sns.relplot(data=window_rms_by_scenario, x='window', y='maxrms', kind='scatter',
                 col='experiment',
                 hue='window')
p2.fig.subplots_adjust(top=0.85)
p2.fig.suptitle(ESM + " window size vs max RMSE by ensemble member")

p2.savefig('./figs/' + ESM + '/' + ESM + '_RMSvsWindow_byScenario.png')


# #############################################################################
## Take a look at time series - tidy data for comparison and plotting
# #############################################################################

# define quick function to smooth and tidy t_anomaly data for easy comparison
# and plotting:
# adding in a column labeling window size, joining in reference values and
# taking the difference.
def smooth_and_tidy(anomalydata, windowsize):
    # do the smoothing:
    smoothed = matchup.calculate_rolling_mean(anomalydata, windowsize)

    # relabel hitorical years as historical,
    smoothed.loc[smoothed['year'] < 2015, 'experiment'] = 'historical'

    # join in the reference values
    anom = smoothed.merge(t_anom_ref,
                          on=['timestep', 'grid_type',
                              'model', 'experiment', 'year', 'variable'],
                          how='left').copy()

    # get the deviations from reference for each value
    anom['diff'] = anom['value'] - anom['refvalue']

    # add an id column for windowsize information
    anom['window'] = windowsize

    return(anom)
    # end function definition

# smooth and tidy a few window sizes of interest
smoothed9 = smooth_and_tidy(t_anomaly, 9)
smoothed15 = smooth_and_tidy(t_anomaly, 15)
smoothed17 = smooth_and_tidy(t_anomaly, 17)
smoothed19 = smooth_and_tidy(t_anomaly, 19)
smoothed21 = smooth_and_tidy(t_anomaly, 21)
smoothed31 = smooth_and_tidy(t_anomaly, 31)

# and tidy the raw data manually because rolling averages don't
# work with windowsize = 0
smoothed0 = t_anomaly.copy()
smoothed0 = smoothed0.merge(t_anom_ref,
                            on=['timestep', 'grid_type',
                                'model', 'experiment', 'year', 'variable'],
                            how='left').copy()
smoothed0['diff'] = smoothed0['value'] - smoothed0['refvalue']
smoothed0['window'] = 0

# and the reference
smoothedref = t_anom_ref.copy()
smoothedref['ensemble'] = 'reference'
smoothedref['value'] = smoothedref['refvalue']
smoothedref['diff'] = 0
smoothedref['window'] = 0


# concatenate it all
smoothed = pd.concat([smoothed0, smoothed9,
                      smoothed15,
                      smoothed17, smoothed19,
                      smoothed21, smoothed31,
                      smoothedref])

# convert the integer windows to strings so that seaborn puts the actual number
# on the legend instead of slices of a range
smoothed['window'] = smoothed['window'].map(str)

# #############################################################################
## Plot time series - single ensemble member, diff window sizes
# #############################################################################

# Filter to one single ensemble member for some plots digging into
# smoothing
single = smoothed[smoothed['ensemble'] == 'r4i1p1f1'].copy()
p4 = sns.relplot(data=single, x='year', y='value',
                 hue='window',
                 col='experiment',
                 kind='line',
                 linewidth=0.6)
p4.fig.subplots_adjust(top=0.85)
p4.fig.suptitle(ESM + " r4i1p1f1")
p4.savefig('./figs/' + ESM + '/' + ESM + '_time_series_r4i1p1f1.png')

# and with fewer smoothing cases:
single2 = single.loc[(single['window'] == '0') | (single['window'] == '9') | (single['window'] == '17')].copy()
p5 = sns.relplot(data=single2, x='year', y='value',
                 hue='window',
                 col='experiment',
                 kind='line',
                 linewidth=0.6)
p5.fig.subplots_adjust(top=0.85)
p5.fig.suptitle(ESM + " r4i1p1f1")
p5.savefig('./figs/' + ESM + '/' + ESM + '_time_series_r4i1p1f1_fewer_windows.png')


# #############################################################################
## Plot time series - single window size, all ensemble members
# #############################################################################

# Filter to one single ensemble member for some plots digging into
# smoothing
window17 = smoothed[smoothed['window'] == '17'].copy()
p6 = sns.relplot(data=window17, x='year', y='value',
                 hue='ensemble',
                 col='experiment',
                 kind='line',
                 linewidth=0.6)
p6.fig.subplots_adjust(top=0.85)
p6.fig.suptitle(ESM + " all ensemble members, smoothing = 17")
p6.savefig('./figs/' + ESM + '/' + ESM + '_time_series_all_ensemble_members17.png')


window9 = smoothed[smoothed['window'] == '9'].copy()
p7 = sns.relplot(data=window9, x='year', y='value',
                 hue='ensemble',
                 col='experiment',
                 kind='line',
                 linewidth=0.6)
p7.fig.subplots_adjust(top=0.85)
p7.fig.suptitle(ESM + " all ensemble members, smoothing = 9")
p7.savefig('./figs/' + ESM + '/' + ESM + '_time_series_all_ensemble_members9.png')


window31 = smoothed[smoothed['window'] == '31'].copy()
p8 = sns.relplot(data=window31, x='year', y='value',
                 hue='ensemble',
                 col='experiment',
                 kind='line',
                 linewidth=0.6)
p8.fig.subplots_adjust(top=0.85)
p8.fig.suptitle(ESM + " all ensemble members, smoothing = 31")
p8.savefig('./figs/' + ESM + '/' + ESM + '_time_series_all_ensemble_members31.png')