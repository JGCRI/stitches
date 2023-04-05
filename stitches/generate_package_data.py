import stitches.make_tas_archive as mk_tas
import stitches.make_matching_archive as mk_match
import stitches.make_pangeo_table as mk_pangeo



def generate_pkg_data(smoothing_window=9, chunk_window=9, add_staggered=False):
    """ Generate all of the internal package data for stitches, the tas archive,
    matching archive, & the table of pangeo files.

    :return: Nothing, running this function should in addition to temporary files
    generate all of the csv files that are included in the prebuilt stitches package.
    """

    # This takes several hours to run.
    mk_tas.make_tas_archive()

    # These two functions run quickly.
    mk_match(smoothing_window=smoothing_window, chunk_window=chunk_window,
                                   add_staggered=add_staggered)
    mk_pangeo()

    return None
