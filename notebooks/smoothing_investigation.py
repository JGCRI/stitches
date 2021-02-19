from stitches.pkgimports import *

# Load the matchup related functions.
import stitches.dev_matchup as matchup

# plotting aesthetic
sns.set_style('whitegrid')

# have pandas print all columns in console
pd.set_option('display.max_columns', None)

# Import the data and select the model to use, I suspect that in the future these will be
# combined into a single function call.
data = matchup.cleanup_main_tgav("./stitches/data/created_data/main_tgav_all_pangeo_list_models.csv")
tgav_data = matchup.select_model_to_emulate("CanESM5", data)

# investigate how many ensemble members per experiment
a = tgav_data[['experiment', 'ensemble']].drop_duplicates().copy()
print(a.groupby('experiment').agg(['count']))

# Calculate the temperature anomaly relative to 1995 - 2014 (IPCC reference period).
t_anomaly = matchup.calculate_anomaly(tgav_data).drop('activity', 1)


# So it turns out that something funky is going on with the ssp534-over values, it looks like
# the time series is incomplete for some reason, not really sure what we want to do with that
# but for now let's just remove it.
t_anomaly = t_anomaly[t_anomaly["experiment"] != "ssp534-over"]


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
                 col='experiment', kind='scatter')
p1.fig.subplots_adjust(top=0.85)
p1.fig.suptitle("CanESM5 reference Tgav anomalies - smoothed across ensemble members")
p1.set_axis_labels('Year', 'Tgav anomaly from 1995-2014, degC')
p1.savefig('canesm5_ensemble_avg_byScenario.png')

window_rms = pd.DataFrame()
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

    # Get that max distance:
    ens_rms = ens_rms.groupby(['model', 'variable', #'experiment',
                              'timestep', 'grid_type']).agg(
        {'rms': lambda x: max(x)}).reset_index().rename(
        columns={'rms':'maxrms'}).copy()

    ens_rms['window'] = windowsize


    # save out that max distance info.
    window_rms = window_rms.append(ens_rms)

# end for loop

# plot that RMS
p3 = sns.relplot(data=window_rms, x='window', y='maxrms', kind='scatter',
                 #col='experiment',
                 hue='window')

#p3.savefig('canesm5_RMSvsWindow_byScenario.png')

p3.savefig('canesm5_RMSvsWindow_allScenarios.png')