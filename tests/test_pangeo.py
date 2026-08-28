"""Tests for the Pangeo CMIP6 catalog interface.

These tests reach out to the live Pangeo catalog on Google Cloud Storage and are
therefore marked ``network``. Previously they were gated behind a ``RUN = "ci"``
class attribute whose false branch asserted ``0 == 0``, meaning they reported as
*passing* in CI while exercising nothing. They now skip visibly instead; run
them with ``pytest --network``.
"""

import pandas as pd
import pytest
import xarray as xr

from stitches.fx_pangeo import fetch_nc, fetch_pangeo_table


@pytest.mark.network
def test_fetch_pangeo_table_returns_dataframe():
    """`fetch_pangeo_table` returns a non-empty DataFrame with expected columns."""
    ptable = fetch_pangeo_table()

    assert isinstance(ptable, pd.DataFrame), "fetch_pangeo_table did not return a DataFrame"
    assert len(ptable) > 0, "Pangeo table is empty"

    for column in ("zstore", "table_id", "activity_id", "source_id", "variable_id"):
        assert column in ptable.columns, f"Pangeo table missing column {column!r}"


@pytest.mark.network
@pytest.mark.slow
def test_fetch_nc_returns_dataset():
    """`fetch_nc` opens a single zarr store from the catalog as an xarray Dataset."""
    ptable = fetch_pangeo_table()

    import_this = ptable.loc[
        (ptable["table_id"] == "Amon") & (ptable["activity_id"] == "ScenarioMIP")
    ].reset_index(drop=True)
    assert len(import_this) > 0, "no Amon/ScenarioMIP entries found in the Pangeo table"

    out = fetch_nc(import_this["zstore"][0])

    assert isinstance(out, xr.Dataset), "problem with fetch_nc"
    assert "time" in out.dims
