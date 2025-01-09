# from utils.load_kg_data import *

# for commodity in ['tungsten', 'lithium']:
#     pl_data = load_minmod_kg(commodity=commodity)
#     pl_data.write_csv(f'/home/yaoyi/pyo00005/Spatial_Entity_Matching/data/raw/{commodity}_all.csv')

import polars as pl
import pandas as pd
from itertools import combinations

pl_data = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/umn-ta2-mineral-site-linkage/_archive_resource/GBW_MRDS.csv', infer_schema_length=0).select(
    pl.col(['OBJECTID', 'COMB_DEP_IDs'])
).with_columns(
    pl.col('COMB_DEP_IDs').str.split('|')
).explode('COMB_DEP_IDs').with_columns(
    pl.col('COMB_DEP_IDs').str.strip_chars()
).with_columns(
    ms_uri = pl.lit('site__mrdata-usgs-gov-mrds__') + pl.col('COMB_DEP_IDs')
)

list_uris = pl_data['ms_uri'].to_list()
list_groups = pl_data['OBJECTID'].to_list()

dict_grouping = dict(zip(list_uris, list_groups))

all_combinations = list(combinations(list_uris, 2))
df = pd.DataFrame(all_combinations, columns =['ms_uri_1', 'ms_uri_2'])
pl_data = pl.from_pandas(df).with_columns(
    uri1_grp = pl.col('ms_uri_1').replace(dict_grouping),
    uri2_grp = pl.col('ms_uri_2').replace(dict_grouping),
).with_columns(
    pl.when(pl.col('uri1_grp') == pl.col('uri2_grp')).then(1).otherwise(0).alias('ground_truth')
).drop(['uri1_grp', 'uri2_grp'])

pl_data.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/umn-ta2-mineral-site-linkage/tests/data/w_gbw.csv')

print(pl_data)