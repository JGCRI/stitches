# Import packages
import pandas as pd
import stitches as stitches
import pkg_resources

# Load the archive data we want to match on.
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
data = pd.read_csv(archive_path)

# Select the some data to use as the target data
target_data = data[data["model"] == 'BCC-CSM2-MR']
target_data = target_data[target_data["experiment"] == 'ssp245']
target_data = target_data[target_data["ensemble"].isin(['r4i1p1f1', 'r1i1p1f1'])]
target_data = target_data.reset_index(drop=True)

# Select the data to use as our archive.
archive_data = data[data['model'] == 'BCC-CSM2-MR'].copy()

# Here is an example of how to make a recipe using the make recipe function, is basically wraps
# all of the matching & formatting steps into a single function. Res can be set to "mon" or "day"
# to indicate the resolution of the stitched data. Right now day & res are both working!
# However we may run into issues with different types of calendars.
rp = stitches.make_recipe(target_data=target_data, archive_data=archive_data, N_matches=2, res="day", tol=0.1)
out = stitches.gridded_stitching(".", rp)

# Now let's try with additional variables, note these should be variables other than tas.
rp = stitches.make_recipe(target_data=target_data, archive_data=archive_data, N_matches=2, res="day",
                          non_tas_variables=["pr"])
out2 = stitches.gridded_stitching(".", rp)
