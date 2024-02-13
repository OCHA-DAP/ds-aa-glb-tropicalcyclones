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

# EMDAT

From [https://public.emdat.be/data](https://public.emdat.be/data)

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pycountry

from src.datasources import emdat, gaul, ibtracs
```

```python
df = emdat.load_raw_emdat()
```

```python
df.columns
```

```python
def iso3_to_iso2(iso3):
    backup = {"SPI": "ES"}
    try:
        return pycountry.countries.get(alpha_3=iso3).alpha_2
    except AttributeError:
        return backup.get(iso3)
```

```python
df["iso2"] = df["ISO"].apply(iso3_to_iso2)
```

```python
total_damage = df.groupby("iso2")["Total Damage, Adjusted ('000 US$)"].sum()
```

```python
total_damage
```

```python
adm0 = gaul.load_gaul()
```

```python
adm0
```

```python
adm0_damage = adm0.merge(total_damage, right_index=True, left_on="isocode")
```

```python
total_damage
```

```python
adm0_damage.plot(column="Total Damage, Adjusted ('000 US$)")
```

```python

```
