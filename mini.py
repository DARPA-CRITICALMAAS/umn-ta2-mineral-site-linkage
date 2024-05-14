# # import polars as pl

# # # pl_tmp = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/GBW_MRDS.csv', ignore_errors=True).select(
# # #     dep_ids = pl.col('COMB_DEP_IDs')
# # # )

# # # group_list = list(range(pl_tmp.shape[0]))

# # # pl_tmp = pl_tmp.with_columns(
# # #     GroupID = pl.Series(group_list)
# # # ).with_columns(
# # #     pl.col('dep_ids').str.split('|')
# # # ).explode('dep_ids').with_columns(
# # #     pl.col('dep_ids').str.strip_chars()
# # # )

# # # list_tmp_ids = pl_tmp['dep_ids'].to_list()

# # # pl_data = minmod_kg_check(source_name='https://mrdata.usgs.gov/mrds').filter(
# # #     pl.col('record_id').is_in(list_tmp_ids)
# # # )

# # # pl_data.write_csv('./full_MRDS.csv')
# # # print(pl_data)

# # # print(pl_tmp)

# # pl_data = pl.read_csv('./full_MRDS.csv')

# # print(pl_data)

# import pickle

# file_name = './resource/attribute_archive.pkl'

# with open(file_name, 'rb') as handle:
#     input_data = pickle.load(handle)
#     print(input_data)


# from utils.load_kg_data import *

# pl_data = load_minmod_kg('germanium') #55
# pl_data = load_minmod_kg('gallium') #53

# print(pl_data)
# pl_data = pl_data.filter(
#     pl.col('source_id') == 'https://doi.org/10.5066/P96MMRFD'
# )

import pickle

dictionary_sample = {'record_id': ['dep_id', 'Site_ID', 'ARDF_number']}

with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_archive.pkl', 'wb') as handle:
    pickle.dump(dictionary_sample, handle, protocol=pickle.HIGHEST_PROTOCOL)

# import polars as pl
# import geopandas as gpd

# csv_format = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/RAW_DATA/https:__mrdata.usgs.gov_mrds/mrds.csv', ignore_errors=True).with_columns(
#     pl.col('dep_id').cast(pl.Utf8)
# )
# shape_file = gpd.read_file('/home/yaoyi/pyo00005/CriticalMAAS/src/RAW_DATA/https:__mrdata.usgs.gov_mrds/mrds-trim/mrds-trim.shp')[['DEP_ID', 'CODE_LIST']]
# shape_file = pl.from_pandas(shape_file).select(
#     dep_id = pl.col('DEP_ID'),
#     CODE_LIST = pl.col('CODE_LIST')
# )

# pl_data = pl.concat(
#     [csv_format, shape_file],
#     how='align'
# )

# pl_data.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/RAW_DATA/https:__mrdata.usgs.gov_mrds/mrds.csv')