def build_test_data():
    """
    Build test data for the test suite.

    This function creates and writes out test data files used in the test suite.
    It reads the matching archive CSV, filters the data for specific model,
    experiment, and ensemble values, and then writes out the data after
    performing neighborhood matching.

    :return: None
    """
    archive_path = resources.files("stitches") / "data" / "matching_archive.csv"
    data = pd.read_csv(archive_path)

    target_data = data[data["model"] == "BCC-CSM2-MR"]
    target_data = target_data[target_data["experiment"] == "ssp245"]
    target_data = target_data[target_data["ensemble"] == "r1i1p1f1"]
    target_data = target_data.reset_index(drop=True)

    archive_data = data[data["model"] == "BCC-CSM2-MR"].copy()
    match_df = match_neighborhood(
        target_data, archive_data, tol=0.1, drop_hist_duplicates=False
    )
    match_path = "test-match_w_dup.csv"
    match_df.to_csv(match_path, index=False)

    match_df = match_neighborhood(
        target_data, archive_data, tol=0.1, drop_hist_duplicates=True
    )
    match_path = "test-match.csv"
    match_df.to_csv(match_path, index=False)

    path = "test-target_dat.csv"
    tar = pd.read_csv(path)

    path = "test-archive_dat.csv"
    arch = pd.read_csv(path)

    match_df = match_neighborhood(tar, arch, tol=0.1, drop_hist_duplicates=True)
    match_path = "test-match_dat.csv"
    match_df.to_csv(match_path, index=False)
