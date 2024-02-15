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

# HRP country impact

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import pandas as pd

from src.datasources import humanitarianinfo, gaul, ibtracs
```

```python
# humanitarianinfo.download_operations_list()
```

```python
# humanitarianinfo.process_operations_list()
```

```python
adm0 = gaul.load_gaul()
adm0 = adm0.sort_values("name0")
adm0["geometry"] = adm0["geometry"].simplify(0.1)
```

```python
for iso2, row in adm0.set_index("isocode").iterrows():
    print([iso2, row["name0"]])
```

```python
df = humanitarianinfo.load_operations_list()
hrp = df[df["Plan type"] == "HRP"]
hrp = hrp.merge(
    adm0[["asap0_id", "isocode", "geometry"]],
    right_on="isocode",
    left_on="ISO2",
)
hrp
```

```python
tracks = ibtracs.load_ibtracs_with_wmo_wind()
cyclones = tracks.groupby("sid").first().reset_index()
cyclones["year"] = cyclones["time"].dt.year
max_year = cyclones["year"].max()
```

```python
thresholds = ibtracs.load_thresholds()
thresholds = thresholds.merge(cyclones[["sid", "year", "name"]], on="sid")
thresholds
```

```python
d_thresh = 250
s_thresh = 95
year_thresh = 2000
total_years = max_year - year_thresh

triggered = thresholds[
    (thresholds["d_thresh"] == d_thresh)
    & (thresholds["s_thresh"] == s_thresh)
    & (thresholds["year"] >= year_thresh)
]
rp = triggered.groupby("asap0_id").size().reset_index(name="count")
rp["count_per_year"] = rp["count"] / total_years
rp = rp.merge(
    adm0[["asap0_id", "name0", "isocode", "geometry"]],
    on="asap0_id",
    how="outer",
)
cols = ["count", "count_per_year"]
rp[cols] = rp[cols].fillna(0)
# tag countries with HRP
# HRP list taken from 2024 Global HNO
# need to manually add Gaza and West Bank since they do not have a isocode
rp["hrp"] = rp.apply(
    lambda row: row["isocode"] in humanitarianinfo.HRP_2024_ISO2S
    or row["name0"] in ["Gaza Strip", "West Bank"],
    axis=1,
)
rp = gpd.GeoDataFrame(
    data=rp.drop(columns=["geometry"]), geometry=rp["geometry"]
)
```

```python
rp
```

```python
rp.hist("count", bins=20)
```

```python
fig, ax = plt.subplots(figsize=(15, 15))
adm0.boundary.plot(ax=ax, linewidth=0.05, color="k")

cutoffs = [4, 8]

bins = [-1, 0, *cutoffs, rp["count"].max()]
labels = [
    "0",
    f"1-{cutoffs[0]}",
    f"{cutoffs[0]+1}-{cutoffs[1]}",
    f">{cutoffs[1]}",
]
rp["count_bins"] = pd.cut(rp["count"], bins=bins, labels=labels)

hrp_colors = {
    "0": "white",
    f"1-{cutoffs[0]}": "yellow",
    f"{cutoffs[0]+1}-{cutoffs[1]}": "darkorange",
    f">{cutoffs[1]}": "red",
}
nonhrp_colors = {
    "0": "white",
    f"1-{cutoffs[0]}": "gainsboro",
    f"{cutoffs[0]+1}-{cutoffs[1]}": "darkgray",
    f">{cutoffs[1]}": "gray",
}

rp["plot_color"] = rp.apply(
    lambda row: hrp_colors.get(row["count_bins"])
    if row["hrp"]
    else nonhrp_colors.get(row["count_bins"]),
    axis=1,
)

rp.plot(color=rp["plot_color"], ax=ax)

ax.axis("off")

print(hrp_colors)
```

```python
rp[rp["hrp"]]
```

```python

```
