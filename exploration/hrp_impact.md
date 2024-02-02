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
from src.datasources import humanitarianinfo, gaul
```

```python
humanitarianinfo.download_operations_list()
```

```python
humanitarianinfo.process_operations_list()
```

```python
df = humanitarianinfo.load_operations_list()
hrp = df[df["Plan type"] == "HRP"]
```

```python
hrp
```

```python

```
