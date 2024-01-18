import os
import urllib
from pathlib import Path

import geopandas as gpd
import pandas as pd
import xarray as xr
from shapely import Point
from tqdm import tqdm

from src.datasources import gaul

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
    df["row_id"] = df.index
    df.to_parquet(IBTRACS_PROC_DIR / f"{filestem}.parquet", index=False)


def load_ibtracs_with_wmo_wind():
    """Load IBTrACS data with WMO wind speed."""
    load_path = IBTRACS_PROC_DIR / "ibtracs_with_wmo_wind.parquet"
    df = pd.read_parquet(load_path)
    geometry = [Point(lon, lat) for lon, lat in zip(df["lon"], df["lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf


def calculate_adm0_distances():
    all_adm0 = gaul.load_gaul(admin_level=0)
    all_adm0 = all_adm0.to_crs(3857).set_index("asap0_id")
    gdf = load_ibtracs_with_wmo_wind()
    gdf = gdf.to_crs(3857)
    save_dir = IBTRACS_PROC_DIR / "adm0_distances"
    for adm0 in tqdm(all_adm0.index):
        adm0_geo = all_adm0.loc[adm0, "geometry"]
        distances = pd.DataFrame()
        distances["distance (m)"] = gdf.geometry.distance(adm0_geo).astype(int)
        distances["row_id"] = gdf["row_id"]
        distances["asap0_id"] = adm0
        filename = f"adm0_{adm0}_distances.parquet"
        distances.to_parquet(save_dir / filename, index=False)


def concat_adm0_distances():
    load_dir = IBTRACS_PROC_DIR / "adm0_distances"
    all_adm0 = gaul.load_gaul(admin_level=0)
    all_adm0 = all_adm0.set_index("asap0_id")
    dfs = []
    for adm0 in tqdm(all_adm0.index):
        filename = f"adm0_{adm0}_distances.parquet"
        df_in = pd.read_parquet(load_dir / filename)
        dfs.append(df_in)

    all_distances = pd.concat(dfs, ignore_index=True)
    save_dir = DATA_DIR / "public" / "processed" / "glb" / "ibtracs"
    filename = "all_adm0_distances.parquet"
    all_distances.to_parquet(save_dir / filename)


def load_all_adm0_distances():
    return pd.read_parquet(IBTRACS_PROC_DIR / "all_adm0_distances.parquet")


def calculate_thresholds():
    speeds = load_ibtracs_with_wmo_wind()
    distances = load_all_adm0_distances()
    distances = distances.merge(
        speeds[["row_id", "sid", "wmo_wind"]], on="row_id"
    )
    d_threshs = range(0, 501, 10)
    s_threshs = range(0, int(speeds["wmo_wind"].max() + 1), 5)

    dfs = []
    for d_thresh in tqdm(d_threshs):
        for s_thresh in s_threshs:
            dff = distances[
                (distances["wmo_wind"] >= s_thresh)
                & (distances["distance (m)"] <= d_thresh * 1000)
            ]
            dff = dff.groupby(["sid", "asap0_id"]).first().reset_index()
            df_add = dff[["sid", "asap0_id"]]
            df_add["d_thresh"] = d_thresh
            df_add["s_thresh"] = s_thresh
            dfs.append(df_add)

    all_thresholds = pd.concat(dfs, ignore_index=True)
    filename = "all_adm0_thresholds.parquet"
    all_thresholds.to_parquet(IBTRACS_PROC_DIR / filename, index=False)


def load_thresholds():
    filename = "all_adm0_thresholds.parquet"
    return pd.read_parquet(IBTRACS_PROC_DIR / filename)
