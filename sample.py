# # # import polars as pl
# # # import pickle5 as pickle
# # # import numpy as np
# # # import hdbscan
# # # import geopandas as gpd
# # # from sklearn.cluster import HDBSCAN

# # # file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MRDS.csv'

# # # pl_mrds = pl.read_csv(file_path)

# # # total_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MineralSites.csv'

# # # pl_combined = pl.read_csv(total_path)

# # # pl_combined = pl_combined.with_columns(
# # #     splitted_source = pl.col('source_ID').str.split('; ')
# # # ).drop(['loc_source', 'source_ID']).explode('splitted_source').filter(
# # #     pl.col('splitted_source').str.contains('MRDS')
# # # ).rename({'splitted_source':'source_ID'}).filter(
# # #     pl.col('source_ID') != 'MRDS_384',
# # #     pl.col('source_ID') != 'MRDS_385',
# # #     pl.col('source_ID') != 'MRDS_386'
# # # )

# # # pl_ground_truth = pl_combined.select(
# # #     pl.col('source_ID').str.split('_').list.to_struct(),
# # #     GroupID = pl.col('OID_')
# # # ).unnest('source_ID').rename({'field_0':'source_id', 'field_1':'record_id'}).select(
# # #     pl.col('source_id'),
# # #     pl.col('record_id').cast(pl.Int64),
# # #     pl.col('GroupID')
# # # ).sort('record_id')

# # # mrds_map_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MRDS_map.csv'

# # # pl_mrds_map = pl.read_csv(mrds_map_path)
# # # pl_mrds_map = pl_mrds_map.filter(
# # #     pl.col('source_ID') != 'MRDS_384',
# # #     pl.col('source_ID') != 'MRDS_385',
# # #     pl.col('source_ID') != 'MRDS_386'
# # # ).select(
# # #     pl.col('source_ID').str.split('_').list.to_struct(),
# # #     pl.col('dep_id')
# # # ).unnest('source_ID').rename({'field_0':'source_id', 'field_1':'record_id'}).select(
# # #     pl.col('record_id').cast(pl.Int64),
# # #     pl.col('dep_id')
# # # ).sort('record_id')

# # # pl_unique_id = pl_mrds_map.select(
# # #     pl.col('dep_id')
# # # )

# # # pl_ground_truth = pl.concat(
# # #     [pl_ground_truth, pl_unique_id],
# # #     how='horizontal'
# # # ).select(
# # #     pl.col('GroupID'),
# # #     pl.col('dep_id')
# # # ).group_by(
# # #     'GroupID'
# # # ).agg(
# # #     [pl.col('dep_id')]
# # # ).with_columns(
# # #     count = pl.col('dep_id').list.len()
# # # ).filter(
# # #     pl.col('count') > 1
# # # ).explode(
# # #     'dep_id'
# # # ).sort('dep_id')

# # # pl_truth_group = pl_ground_truth.select(
# # #     pl.col('GroupID')
# # # )

# # # list_id = pl_ground_truth['dep_id'].to_list()

# # # pl_ground_truth['dep_id']

# # # pd_ground_truth = pl_ground_truth.to_pandas()

# # # pd_ground_truth.to_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MRDS_gt_w_group.csv')

# # # pl_ground_truth.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MRDS_gt.csv', separator=",")

# # # ids = '10019943|10196701|10289747|10022920|10071085|10110537|10091791|10197331|10193406|10071595|10105736|10145076|10010817|10123520|10123900|10019262|10168068|10071001|10270010|10010819|10270495|10197696|10008992|10090571|10071837|10196947|10148355|10123742|10070444'

# # # pl_mrds = pl_mrds.filter(
# # #     pl.col('dep_id').str.contains(ids)
# # # ).sort('dep_id')

# # # pl_mrds = pl.concat(
# # #     [pl_mrds, pl_truth_group],
# # #     how='horizontal'
# # # ).select(
# # #     pl.all().cast(pl.Utf8)
# # # ).group_by(
# # #     'GroupID'
# # # ).agg(
# # #     [pl.all()]
# # # ).select(
# # #     pl.col(['url','site_name','region', 'country', 'state', 'county', 'com_type', 'commod1', 'commod2', 'commod3', 'oper_type', 'dep_type', 'prod_size', 'dev_stat', 'ore', 'gangue', 'other_matl', 'orebody_fm', 'work_type', 'model', 'alteration', 'conc_proc', 'names', 'ore_ctrl', 'reporter', 'hrock_unit', 'hrock_type', 'arock_unit', 'arock_type', 'structure', 'tectonic', 'ref', 'yr_fst_prd', 'yr_lst_prd', 'dy_ba', 'disc_yr', 'prod_yrs', 'discr', 'dep_type1', 'source', 'map_no', 'district', 'DMEA_file', 'Prod_Resou', 'Sys_Class', 'Class_Sour'])
# # # )

# # # pl_mrds = pl_mrds.with_columns(
# # #     name_total = pl.col('site_name').list.concat('names'),
# # #     commodity_total = pl.col('commod1').list.concat(['commod2', 'commod3'])
# # # )

# # # print(pl_mrds[['name_total', 'commodity_total']])

# import polars as pl
# import geopandas as gpd
# import pandas as pd
# import pickle

# # gbw_file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl/MRDS_GBW.pkl'
# # mrds_gbw_ground_truth = '/home/yaoyi/pyo00005/CriticalMAAS/src/archive/src/GBW_MRDS.csv'
# # usmin_tungsten_file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl/USMIN.pkl'
# # usmin_cobalt_file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/goal/USGS_Cobalt_US.gdb'
# # usmin_tunsten_full_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/goal/USMIN.gdb'
# # usmin_idmt_ground_truth = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MineralSites.csv'
# mrds_full = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/full_MRDS.pkl'
# # mrds_mvtzinc_file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/MVTZinc/MRDS.csv'
# # alaska_zinc_file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/MVTZinc/10.5066_P96MMRFD.csv'

# with open(mrds_full, 'rb') as handle:
#     dataframe = pickle.load(handle)

# # dataframe = dataframe.drop(['geometry'], axis=1)

# # dataframe = pl.from_pandas(dataframe)


# dataframe1 = dataframe.filter(
#     pl.col('commod1').str.contains('Zinc')
# )
# dataframe2 = dataframe.filter(
#     pl.col('commod2').str.contains('Zinc')
# )
# dataframe3 = dataframe.filter(
#     pl.col('commod3').str.contains('Zinc')
# )

# dataframe = pl.concat(
#     [dataframe1, dataframe2, dataframe3],
#     how='vertical'
# )

# dataframe = dataframe.filter(
#     pl.col('country').str.contains('Australia|United States|Ireland|Iran|Spain|China|Russia|Morocco|Canada|Austria|Peru|Kyrgyzstan|Australia|Algeria|Green'),
#     pl.col('site_name').str.contains('Admiral|Navan|Reocin|Fankou|Pavlov|Buick|Elura|Daliangzi|Touissit|Polaris|Bleiberg|Lisheen|Prairie|Vicente|Florida|gayna|Sumsar|Tian|Cadjebut|Abed|Pillara|Blendvale|Angel|Urultun|Magmont|Nanisivik|Huayuan|Silvermines|Galmoy|Emarat')
# )

# # print(dataframe)

# # dataframe.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/data/Zinc/MRDS_sub.csv')

# dataframe = dataframe.filter(
#     pl.col('country').str.contains('Iran'),
#     # pl.col('site_name').str.contains('Vicente|Florida')
# ).select(
#     pl.col('site_name')
# )

# print(dataframe)

# # with open('/home/yaoyi/pyo00005/CriticalMAAS/src/data/Zinc/MRDS.csv', 'wb') as handle:
# #     pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

# # geo = dataframe.select(
# #     latitude = pl.col('Lat_NAD83'),
# #     longitude = pl.col('Long_NAD83'),
# # )

# # dataframe = dataframe.select(
# #     unique_id = pl.col('Fep_ID'),
# #     source = pl.lit('USMIN'),
# #     name = pl.col('Ftr_Name'),
# # ).to_pandas()


# # gdf = gpd.GeoDataFrame(
# #     dataframe, geometry=gpd.points_from_xy(geo['longitude'], geo['latitude']), crs='NAD83')

# # # print(gdf)

# # # dataframe = pl.read_csv(mrds_mvtzinc_file_path)

# # # geo = dataframe.select(
# # #     latitude = pl.col('latitude'),
# # #     longitude = pl.col('longitude'),
# # # )

# # # dataframe = dataframe.select(
# # #     unique_id = pl.col('dep_id'),
# # #     source = pl.lit('MRDS'),
# # #     name = pl.col('site_name'),
# # # ).to_pandas()

# # # gdf = gpd.GeoDataFrame(
# # #     dataframe, geometry=gpd.points_from_xy(geo['longitude'], geo['latitude']))

# # gdf.to_file('/home/yaoyi/pyo00005/CriticalMAAS/src/data/6_month_hackathon/raw/usmin_idmt_tungsten.geojson', drive='GeoJSON')

# # # print(gdf)

# # # dataframe = pl.read_csv(mrds_full, ignore_errors=True)

# # # dataframe = dataframe.filter(
# # #     pl.col('source_ID').str.contains('USMIN|MRDS')
# # # )

# # # dataframe = dataframe.select(
# # #     pl.col('COMB_DEP_IDs').str.split(' | ')
# # # ).explode('COMB_DEP_IDs')

# # # dataframe = dataframe.select(
# # #     pl.col(['LONGITUDE', 'LATITUDE'])
# # # )

# # import polars as pl

# # data = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/MVTZinc/MRDS.csv'
# # dictionary = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/MVTZinc/dict_MRDS.csv'

# # pl_data = pl.read_csv(data)
# # pl_data = pl_data.drop(['dep_id', 'latitude', 'longitude'])
# # data_col = pl_data.columns

# # pl_dictionary = pl.read_csv(dictionary)
# # dictionary_col = pl_dictionary['attribute_label'].to_list()


# # # print(data_col)

# # # print("Columns to form documents", len(data_col))
# # # print(data_col)

# # print("Columns unavailable", len(list(set(data_col) - set(dictionary_col))))
# # print(list(set(data_col) - set(dictionary_col)))

# import pandas as pd
# import polars as pl
# # from json import loads, dumps

# # file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/outputs/MVT_Zinc_ver0.json'
# file_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/outputs/MVT_Zinc_ver2.json'

# # pl_data = pl.read_json(file_path)

# # print(pl_data)

# df = pd.read_json(file_path, lines=True)

# print(df)
# print(df.columns)

# print(df.columns)

# new_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/outputs/MVT_Zinc_ver2.json'

# df = df.to_json(new_path, orient='records', lines=True, default_handler=str)

# json_data = loads(df)
# obj_data = dumps(json_data, indent=4)

# with open(new_path, 'w') as handle:
#     handle.write(obj_data)

# print(df.columns)