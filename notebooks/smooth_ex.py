from stitches.pkgimports import *

# Load the matchup related functions.
import stitches.dev_matchup as matchup


# Import the data and select the model to use, I suspect that in the future these will be
# combined into a single function call.
data = matchup.cleanup_main_tgav("./stitches/data/created_data/main_tgav_all_pangeo_list_models.csv")
tgav_data = matchup.select_model_to_emulate("CanESM5", data)

# Calculate the temperature anomaly relative to 1995 - 2014 (IPCC reference period).
t_anomaly = matchup.calculate_anomaly(tgav_data)

# So it turns out that something funky is going on with the ssp534-over values, it looks like
# the time series is incomplete for some reason, not really sure what we want to do with that
# but for now let's just remove it.
t_anomaly = t_anomaly[t_anomaly["experiment"] != "ssp534-over"]


# Now that we have the temp anomaly calculate the rolling mean with three different windows
# and plot them!
out1 = matchup.calculate_rolling_mean(t_anomaly, 5)
out2 = matchup.calculate_rolling_mean(t_anomaly, 10)
out3 = matchup.calculate_rolling_mean(t_anomaly, 15)


plt.scatter(out1.year, out1.value, label="stars", color="red", s=30)
plt.scatter(out2.year, out2.value, label="stars", color="green", s=30)
plt.scatter(out3.year, out3.value, label="stars", color="blue", s=30)
plt.show()
