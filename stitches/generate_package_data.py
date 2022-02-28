# Single function to generate package data
# honestly not sure if this this working!! fml!
# Load functions
import make_tas_archive as mk_tas
import make_matching_archive as mk_match
import make_pangeo_table as mk_pangeo



def generate_pkg_data():
    """ Generate all of the internal package data for stitches, the tas archive,
    matching archive, & the table of pangeo files.

    :return: Nothing, running this function should in addition to temporary files
    generate all of the csv files that are included in the prebuilt stitches package.
    """

    # This takes several hours to run.
    mk_tas.make_tas_archive()

    # These two functions run quickly.
    mk_match.make_matching_archive()
    mk_pangeo.make_pangeo_table()

    return None
