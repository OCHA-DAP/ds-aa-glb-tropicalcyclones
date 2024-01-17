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
import pandas as pd
from climada.hazard import TCTracks
from tqdm.notebook import tqdm

from src.datasources import gaul, ibtracs
```

```python
ibtracs.download_ibtracs()
```

```python
ibtracs.process_all_ibtracs()
```

```python
df = ibtracs.load_ibtracs_with_wmo_wind()
```

```python
df
```

```python
adm0 = gaul.load_gaul(admin_level=0)
```

```python
adm0.plot()
```

```python

```
