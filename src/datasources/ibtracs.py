import os
import urllib
from pathlib import Path

import pandas as pd
import xarray as xr

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
IBTRACS_RAW_DIR = DATA_DIR / "public" / "raw" / "glb" / "ibtracs"
IBTRACS_PROC_DIR = DATA_DIR / "public" / "processed" / "glb" / "ibtracs"


def download_ibtracs():
    """Download IBTrACS data."""
    url = (
        "https://www.ncei.noaa.gov/data/"
        "international-best-track-archive-for-climate-stewardship-ibtracs/"
        "v04r00/access/netcdf/IBTrACS.ALL.v04r00.nc"
    )
    download_path = IBTRACS_RAW_DIR / "IBTrACS.ALL.v04r00.nc"
    urllib.request.urlretrieve(url, download_path)


def load_all_ibtracs():
    """Load IBTrACS data from NetCDF file."""
    load_path = IBTRACS_RAW_DIR / "IBTrACS.ALL.v04r00.nc"
    return xr.load_dataset(load_path)


def process_all_ibtracs():
    ds = load_all_ibtracs()
    subset_vars = ["sid", "wmo_wind", "name"]
    ds_subset = ds[subset_vars]
    str_vars = ["name", "sid"]
    ds_subset[str_vars] = ds_subset[str_vars].astype(str)
    ds_subset["time"] = ds_subset["time"].astype("datetime64[s]")
    df = ds_subset.to_dataframe().dropna().reset_index()
    cols = ["time", "lat", "lon", "wmo_wind", "name", "sid"]
    df = df[cols]
    filestem = "ibtracs_with_wmo_wind"
    df.to_parquet(IBTRACS_PROC_DIR / f"{filestem}.parquet", index=False)


def load_ibtracs_with_wmo_wind():
    """Load IBTrACS data with WMO wind speed."""
    load_path = IBTRACS_PROC_DIR / "ibtracs_with_wmo_wind.parquet"
    return pd.read_parquet(load_path)
