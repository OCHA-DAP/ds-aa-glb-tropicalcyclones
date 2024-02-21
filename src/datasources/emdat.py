import os
from pathlib import Path

import pandas as pd

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
EMDAT_RAW_DIR = DATA_DIR / "private" / "raw" / "glb" / "emdat"
EMDAT_PROC_DIR = DATA_DIR / "private" / "processed" / "glb" / "emdat"


def load_raw_emdat():
    filename = "emdat-tropicalcyclone-2000-2022.xlsx"
    return pd.read_excel(EMDAT_RAW_DIR / filename)


def load_processed_emdat():
    filename = "emdat-tropicalcyclone-2000-2022-sids.csv"
    return pd.read_csv(EMDAT_PROC_DIR / filename)
