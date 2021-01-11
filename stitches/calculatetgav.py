from stitches.pkgimports import *

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

