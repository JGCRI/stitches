# Define the functions that are used in the matching process.

import stitches.fx_util as util
import numpy as np


# Internal fx
def internal_dist(fx_pt, dx_pt, archivedata, tol=0):
    """ This function calculates the euclidean distance between the target values (fx and dx)
    and the archive values contained in the data frame. It will be used to help select which
    of the archive values best matches the target values. To ensure a consisten unit across
    all dimensions of the space, dx is updated to be windowsize*dx so that it has units of
    degC. This results in a distance metric (Euclidean/l2) in units of degC.
    Could _very_ easily make that choice of unit consistency optional via arg and if-statement.

        :param fx_pt:          a single value of the target fx value
        :param dx_pt:          a single value of the target dx value
        :param archivedata:    a data frame of the archive fx and dx values
        :param archivedata:    a data frame of the archive fx and dx values
        :param tol:            a tolerance for the neighborhood of matching. defaults to 0 degC - only the nearest-neighbor is returned

        :return:               a data frame with the target data and the corresponding matched archive data.
    """

    # Check the inputs
    util.check_columns(archivedata, {'model', 'experiment', 'variable',
                                     'ensemble', 'start_yr', 'end_yr', 'year', 'fx', 'dx'})

    # Compute the window size of the archive data to use to update
    # dx values to be windowsize*dx so that it has units of degC
    windowsize = max(archivedata['end_yr'] - archivedata['start_yr'])

    # TODO ACS do we need to implement this check?
    # # For now comment out the code that enforces that there is only type of window size,
    # # however we do know that when the full ts is not divisible by a window size of 9
    # # or what not then we will run into issues with this. We do know that is will be
    # # important to ennsure that the size of the target chunk and archive chunks are equivalent.
    # TODO address this issue at a latter time
    # if(length(windowsize) > 1){
    #  stop("you've made your archive by chunking with multiple window sizes, don't do that!")
    # }

    # Update the names of the columns in the archive data.
    archivedata.columns = 'archive_' + archivedata.columns

    # calculate the euclidean distance between the target point
    # and the archive observations.
    # Calculating distance d(target, archive_i) for each point i in the archive.
    # calculating the distance in the dx and fx dimensions separately because
    # now we want to track those in addition to the l2 distance.
    archivedata["dist_dx"] = windowsize *  abs(archivedata["archive_dx"] - dx_pt)
    archivedata["dist_fx"] = abs(archivedata['archive_fx'] - fx_pt)
    archivedata["dist_l2"] = (archivedata["dist_fx"] ** 2 + archivedata["archive_dx"] ** 2) ** .5
    dist = archivedata.copy()

    # this returns the first minimum run into, which is not how we are going to want to do it,
    # we will want some way to keep track of the min and combine to have different realizations
    # or have some random generation. But for now I think that this is probably sufficent.
    #
    # probably don't actually need the if-statement to treat tol=0 separately; in theory, the
    # else condition would return the nearest neighbor for tol=0. But just in case of rounding
    # issues, keeping it separate for now to be sure we can replicate previous behavior.
    if tol == 0:
        index = np.argmin(dist['dist_l2'])

        # if there are multiple matches then an error should be thrown! Why
        # is this not happening for the historical period? The ensemble members of
        # different experiments should be identical to one another!
        if index.size > 1:
            raise TypeError(f"more than one identical match found and you only want the nearest neighbor!")
    else:
        min_dist = dist['dist_l2'][np.argmin(dist['dist_l2'])]
        dist_radius = min_dist + tol
        index = np.where(dist["dist_l2"].values <= dist_radius)

    # TODO is there an issue when only the nearest neighboor is being returned? because then it is returned as a series instad of a data frame...
    out = dist.loc[index]
    return out
