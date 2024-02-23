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
import xarray as xr
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
last3years = xr.load_dataset(
    ibtracs.IBTRACS_RAW_DIR / "IBTrACS.last3years.v04r00.nc"
)
```

```python
allyears = ibtracs.load_all_ibtracs()
```

```python
allyears.isel(storm=-1)
```

```python
ibtracs.process_all_ibtracs()
```

```python
ibtracs.process_all_ibtracs("usa")
```

```python
ibtracs.calculate_adm0_distances()
```

```python
ibtracs.calculate_adm0_distances(
    wind_provider="usa", start_year=2000, end_year=2023
)
```

```python
ibtracs.concat_adm0_distances("usa")
```

```python
ibtracs.
```

```python
usa_wind = ibtracs.load_ibtracs_with_wind("usa")
```

```python
usa_wind.iloc[-20:]
```

```python
ibtracs.concat_adm0_distances("usa")
```

```python
ibtracs.calculate_thresholds("usa")
```

```python

```
