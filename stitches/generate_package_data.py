import stitches.make_matching_archive as mk_match
import stitches.make_pangeo_table as mk_pangeo
import stitches.make_tas_archive as mk_tas


def generate_pkg_data(smoothing_window=9, chunk_window=9, add_staggered=False,
                     anomaly_startYr=1995, anomaly_endYr=2014):
    """
    Generate all internal package data for stitches.

    This function creates the tas archive, matching archive, and the table of
    pangeo files. It generates all of the CSV files included in the prebuilt
    stitches package and may produce temporary files during the process.

    :param smoothing_window: The smoothing window size.
    :type smoothing_window: int
    :param chunk_window: The chunk window size.
    :type chunk_window: int
    :param add_staggered: Flag to add staggered output.
    :type add_staggered: bool
    :param anomaly_startYr: The start year for anomaly calculation.
    :type anomaly_startYr: int
    :param anomaly_endYr: The end year for anomaly calculation.
    :type anomaly_endYr: int
    :return: None
    """

    # This takes several hours to run.
    mk_tas(anomaly_startYr=anomaly_startYr, anomaly_endYr=anomaly_endYr)

    # These two functions run quickly.
    mk_match(
        smoothing_window=smoothing_window,
        chunk_window=chunk_window,
        add_staggered=add_staggered,
    )
    mk_pangeo()

    return None
    