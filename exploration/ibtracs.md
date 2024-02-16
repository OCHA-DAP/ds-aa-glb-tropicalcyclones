---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-glb-tropicalcyclones
    language: python
    name: ds-glb-tropicalcyclones
---

# IBTrACS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os
from pathlib import Path

import pandas as pd
from climada.hazard import TCTracks
from tqdm.notebook import tqdm

from src.datasources import gaul, ibtracs
```

```python
DATA_DIR_NEW = Path(os.getenv("AA_DATA_DIR_NEW"))
```

```python
ibtracs.download_ibtracs()
```

```python
ibtracs.process_all_ibtracs()
```

```python
ibtracs.calculate_adm0_distances()
```

```python
ibtracs.concat_adm0_distances()
```

```python
ibtracs.calculate_thresholds()
```

```python
speeds = ibtracs.load_ibtracs_with_wmo_wind()
speeds
```

```python
distances = ibtracs.load_all_adm0_distances()
distances = distances.merge(speeds[["row_id", "sid", "wmo_wind"]], on="row_id")
distances
```

```python
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
        df_add = df_add.astype({
            col: 'int32' for col in ["asap0_id", "d_thresh", "s_thresh"]
        })
        dfs.append(df_add)
```

```python
all_thresholds = pd.concat(dfs, ignore_index=True)
```

```python
all_thresholds
```

```python
filename = "all_adm0_thresholds.parquet"
all_thresholds.to_parquet(ibtracs.IBTRACS_PROC_DIR / filename, index=False)
```

```python
codabs = gaul.load_gaul()
```

```python
codabs[codabs["name0_shr"] == "Fiji"].plot()
```

```python
speeds["year"] = speeds["time"].dt.year
```

```python
unique_storms = speeds.groupby("sid").first().reset_index()
unique_storms
```

```python
filename = "all_adm0_thresholds.parquet"
all_thresholds = pd.read_parquet(ibtracs.IBTRACS_PROC_DIR / filename)
```

```python
all_thresholds = all_thresholds.merge(
    unique_storms[["sid", "year"]], on="sid", how="left"
)
```

```python
all_thresholds
```

```python
asap0_id = 116
min_year = 1970

dff = all_thresholds[
    (all_thresholds["asap0_id"] == asap0_id)
    & (all_thresholds["year"] >= min_year)
]
total_years = dff["year"].nunique()
print(total_years)
rp = total_years / dff.groupby(["d_thresh", "s_thresh"]).size()
display(rp)
```

```python

```
