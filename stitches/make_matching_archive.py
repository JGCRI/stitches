# Define the functions used to create the archive that is used in the matching process,
# aka the rate of change (dx) and median value (fx) for the temperature anomoly time series.

# Import packages
import stitches.fx_processing as prep
import stitches.fx_util as util
import pandas as pd
import pkg_resources


def make_matching_archive(smoothing_window=9, chunk_window=9):
    """"
    The function that creates the archive of rate of change (dx) and mean (fx) values for
    from the CMIP6 archive, these the the values that will be using in the matching portion
    of the stitching pipeline.

    :param smoothing_window:   int default set to 9, the size of the smoothing window to be applied to the ts.
    :param chunk_window:   int default set to 9, the size of the chunks of data to summarize with dx & fx.

    :return:               str location of the matching archive file.
    :return:               str location of the matching archive file.
    """
    # Start by loading all of the tas files.
    raw_data = util.load_data_files('data/tas-data')

    # Smooth the anomalies, get the running mean of each time series.
    smoothed_data = prep.calculate_rolling_mean(raw_data, smoothing_window)

    # For each group in the data set go through, chunk and extract the fx and dx␣ values.
    # For now we have to do this with a for loop, to process each model/experiment/ensemble/variable
    # individually.
    data = smoothed_data[["model", "experiment", "ensemble", "year", "variable", "value"]]
    data = data.reset_index(drop=True).copy()
    group_by = ['model', 'experiment', 'ensemble', 'variable']
    out = []
    for key, d in data.groupby(group_by):
        dat = d.reset_index(drop=True).copy()
        dd = prep.chunk_ts(df=dat, n=chunk_window)
        rslt = prep.get_chunk_info(dd)
        out.append(rslt)
    # end of the for loop & concatenate results into a single data frame.
    data = pd.concat(out).reset_index(drop=True)

    outdir_path = pkg_resources.resource_filename('stitches', 'data')
    ofile = outdir_path + "/matching_archive.csv"
    data.to_csv(ofile, index=False)

    return ofile
