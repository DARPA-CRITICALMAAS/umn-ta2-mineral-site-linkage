import os
import polars as pl
from utils.load_kg_data import *

list_pls = []

for i in os.listdir('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas'):
    if i == '.gitattributes':
        continue

    commodity_name = i.split('_sameas')[0]
    print(commodity_name)
    if commodity_name == 'pge':
        commodity_name = 'platinum-group elements'
    
    pl_data = load_minmod_kg(commodity_name)
    list_pls.append(pl_data)

pl_full = pl.concat(
    list_pls,
    how='diagonal_relaxed'
)

pl_full = pl_full.unique(subset=['ms_uri'], keep='first')
pl_full.write_csv('/home/yaoyi/pyo00005/Spatial_Entity_Matching/full.csv')