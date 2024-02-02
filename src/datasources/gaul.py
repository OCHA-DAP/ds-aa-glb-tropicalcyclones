import os
from pathlib import Path

import geopandas as gpd

DATA_DIR = Path(os.getenv("AA_DATA_DIR"))
ASAP_REF_DIR = DATA_DIR / "public" / "raw" / "glb" / "asap" / "reference_data"


def load_gaul(admin_level: int = 0):
    load_path = (
        ASAP_REF_DIR
        / f"gaul{admin_level}_asap_v04"
        / f"gaul{admin_level}_asap.shp"
    )
    return gpd.read_file(load_path)
