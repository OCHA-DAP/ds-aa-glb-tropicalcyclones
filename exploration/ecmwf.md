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

# ECMWF forecasts

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import ibtracs, gaul, ecmwf
```

```python
tracks = ibtracs.load_ibtracs_with_wind("wmo")
cyclones = tracks.groupby("sid").first().reset_index()
cyclones["year"] = cyclones["time"].dt.year
cyclones["nameyear"] = cyclones.apply(
    lambda row: f'{row["name"].capitalize()} {row["year"]}', axis=1
)
```

```python
thresholds = ibtracs.load_thresholds("wmo", 0)
thresholds = thresholds.merge(
    cyclones[["sid", "name", "nameyear", "year"]], on="sid"
)
thresholds = thresholds.merge(cyclones[["sid", "name"]])
thresholds_slb = thresholds[thresholds["asap0_id"] == 170]
```

```python
adm0 = gaul.load_gaul()
```

```python
adm0[adm0["name0"].str.contains("Solomon")]
```

```python
thresholds_slb
```

```python
slb_names = thresholds_slb["name"].str.lower().unique()
```

```python
slb_names
```
