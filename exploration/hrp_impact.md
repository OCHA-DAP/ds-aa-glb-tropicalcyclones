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
import pandas as pd

from src.datasources import humanitarianinfo, gaul, ibtracs
```

```python
humanitarianinfo.download_operations_list()
```

```python
humanitarianinfo.process_operations_list()
```

```python
adm0 = gaul.load_gaul()
adm0
```

```python
df = humanitarianinfo.load_operations_list()
hrp = df[df["Plan type"] == "HRP"]
hrp = hrp.merge(
    adm0[["asap0_id", "isocode"]], right_on="isocode", left_on="ISO2"
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
thresholds = thresholds[thresholds["asap0_id"].isin(hrp["asap0_id"])]
thresholds = thresholds.merge(hrp[["Plans", "asap0_id"]], on="asap0_id")
thresholds = thresholds.merge(cyclones[["sid", "year", "name"]], on="sid")
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
rp = triggered.groupby("Plans").size().reset_index(name="count")
rp["count_per_year"] = rp["count"] / total_years
display(rp)
print(rp["count_per_year"].mean())
```

```python

```
