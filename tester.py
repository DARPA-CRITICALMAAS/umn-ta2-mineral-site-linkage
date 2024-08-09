import geopandas as gpd
import pandas as pd
import polars as pl

# gdb_data = gpd.read_file('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/PP1802_CritMin_polys.shp')
# print(gdb_data)

pl_data = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_pp1802/points.txt').with_columns(
    pl.col('location').replace({'United States of America': 'United States'})
).drop('geom')

pl_data.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_pp1802/main.csv')
print(pl_data)