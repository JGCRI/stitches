###############################################################################
# Adapt Kalyn's code: Define a function to pull a list of big
# models from pangeo based on some query criteria.
###############################################################################

# Count the number of experiment/ensemble members per model for tas and pr,
# this will help us idetify the models we will initally work with for the
# first pass at  Stiches.
#
# Args
#	obj: an intake object (the pangeo archive generated from intake.open_esm_datastore("https://storage.googleapis.com/cmip6/pangeo-cmip6.json"))
# 	var_name: the variable name to look up, either tas or pr
#	experiments: list of experiments of interest to query.
#	table_ids: list of table ids (e.g. Amon, day) to query.
#	min_ensemble_size: the minimum number of acceptable ensemble members.
#
# Returns
#	a data array of the model, experiment count, and the average ensemble members
#   per experiment
def create_count_table(obj, var_name, experiments, table_ids,
                       min_ensemble_size):
    "This function counts and sorts an experiment/ensemble count"  # what comes out of help(create_count_table)
    # Triple quotes lets you go all the way down, including the args, etc
    # param keyword for each argument; can also specify type with the type keyword (throws a warning
    # but should still do testing and asserts.); and return, with description as returns get more complex.
    # same on inputs, if it's complex, describe it.

    # Subset by the experiment and subset the variables.
    query = dict(experiment_id=experiments,
                 table_id=table_ids)
    obj = obj.search(**query)
    subset = obj.search(**dict(variable_id=[var_name])).df[["source_id", "experiment_id", "member_id"]]

    ensemble_count = subset.groupby(["source_id", "experiment_id"]).nunique('member_id')
    ensembles_per_exp = ensemble_count.groupby(["source_id"]).mean("member_id")
    experiment_count = subset[["source_id", "experiment_id"]].groupby(["source_id"]).nunique('experiment_id')

    # join the experiment and ensemble count, but only keep the entries with
    # more than 5 ensemble memebers.
    table = experiment_count.merge(ensembles_per_exp, on="source_id", how="left").reset_index()
    table = table[table["member_id"] > min_ensemble_size]
    table = table.sort_values(by=["experiment_id", "member_id"], ascending=False)
    return table;


# End function definition


###############################################################################
# Helper functions to calculate Tgav from monthly netcdfs, via
# https://gallery.pangeo.io/repos/pangeo-gallery/cmip6/ECS_Gregory_method.html
###############################################################################

def get_lat_name(ds):
    """Figure out what is the latitude coordinate for each dataset."""
    for lat_name in ['lat', 'latitude']:
        if lat_name in ds.coords:
            return lat_name
    raise RuntimeError("Couldn't find a latitude coordinate")


def global_mean(ds):
    """Return global mean of a whole dataset."""
    lat = ds[get_lat_name(ds)]
    weight = np.cos(np.deg2rad(lat))
    weight /= weight.mean()
    other_dims = set(ds.dims) - {'time'}
    return (ds * weight).mean(other_dims)


# ###############################################################################################
# Helper function for doing regression on an xarray of monthly data,
# getting resids, and adding them as a data variable to the original xarray.
# Works on xarray of annual gridded data or xarray of a time series of a
# single month's gridded data xarray.

# Needs a lot less hardcoded into it but it's a start.

def pattern_scale_resids(xarraydata):
    # set up for regression

    # independent var
    x = tmp_Tgav.tas.values.reshape(-1, 1)  # -1 means that calculate the dimension of rows, but have 1 column

    # dependent var
    y = xarraydata.tas.values

    # create an object of the sklearn Linear Regression class
    linear_regressor = LinearRegression()

    # perform regression
    linear_regressor.fit(x, y)

    # make predictions
    y_pred = linear_regressor.predict(x)

    # get residuals
    resids = y - y_pred

    # add them to the original xarray by making a copy and
    # overwriting the tas values. This makes sure
    # everything is the same dimension so that we can just
    # assign this into the original xarraydata
    z = xarraydata.copy()
    z.tas.values = resids
    z = z.rename_vars({'tas': 'tas_resids'})

    # Assign it and return it
    return xarraydata.assign(tas_resids=z.tas_resids)

