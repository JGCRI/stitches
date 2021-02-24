from stitches.pkgimports import *

# ###############################################################################################
# libraries need for pattern scaling:
from sklearn.linear_model import LinearRegression
import itertools


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

