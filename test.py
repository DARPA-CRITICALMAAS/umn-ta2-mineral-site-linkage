# import pickle
# import polars as pl
# import geopandas as gpd
# from json import dump, loads

# full_MRDS_file = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/full_MRDS.csv'
# pl_data = pl.read_csv(full_MRDS_file)

# commodity_code = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/MRDS_commodity_code.csv'
# pl_code = pl.read_csv(commodity_code)
# dict_code = dict(zip(pl_code['ABR'], pl_code['Abrasive']))
# # print(dict_code)

# minmod_code = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/minmod_commodities.csv'
# pl_minmod = pl.read_csv(minmod_code)
# pl_minmod = pl_minmod.with_columns(
#     minmod_mapping = pl.lit('https://minmod.isi.edu/resource/') + pl.col('minmod_id')
# )
# dict_minmod = dict(zip(pl_minmod['CommodityinMRDS'], pl_minmod['minmod_mapping']))

# # print(dict_minmod)

# def drop_null_attributes(dict_processed_attributes:dict, bool_list:list) -> dict:
#     if not bool_list:
#         filtered_attribute_data = {key: value for key, value in dict_processed_attributes.items() if value and value!=""}

#         return filtered_attribute_data
    
#     new_dictionary = {}
#     for key, value in dict_processed_attributes.items():
#         if type(value) is list:
#             empty_list = []

#             for i in value:
#                 empty_list.append(i)

#             new_dictionary[key] = empty_list
#             print(empty_list)
#         else:
#             try:
#                 if value:
#                     new_dictionary[key] = value
#             except:
#                 if value.all():
#                     new_dictionary[key] = value

#     return new_dictionary

# def cleaning(struct_attributes) -> list:
#     empty_list = []

#     for i in struct_attributes['CODE_LIST'].split():
#         empty_list.append(
#             {
#                 "commodity": dict_minmod[dict_code[i]],
#                 "observed_commodity": dict_code[i],
#                 "reference":{
#                     "document":{
#                         "uri":'https://mrdata.usgs.gov/mrds/'
#                     }
#                 }
#             }
#         )

#     return empty_list

# def to_dictionary(list_deposit:list) -> list:
#     empty_list = []

#     for i in list_deposit:
#         empty_list.append({'observed_name': i.strip()})
#     return empty_list

# # https://minmod.isi.edu/resource/
# pl_portion = pl_data.select(
#     record_id = pl.col('dep_id'),
#     name = pl.col('site_name'),
#     location = pl.lit('POINT (') + pl.col('longitude').cast(pl.Utf8) + pl.lit(', ') + pl.col('latitude').cast(pl.Utf8) + pl.lit(')'),
#     country = pl.col('country'),
#     state_or_province = pl.col('state'),
#     deposit_type_candidate = pl.col('dep_type').str.split(',').map_elements(to_dictionary),
#     mineral_inventory = pl.struct(pl.col(['commod1', 'commod2', 'commod3', 'CODE_LIST'])).map_elements(cleaning)
# ).with_columns(
#     source_id = pl.lit('MRDS'),
#     crs = pl.lit('EPSG:4326'),
# ).with_columns(
#     location_info = pl.struct(pl.col(['location', 'crs', 'country', 'state_or_province'])),
# ).drop(['location', 'crs', 'country', 'state_or_province', 'uri', 'document'])

# # print(pl_portion)

# def clean_nones(value):
#     """
#     Recursively remove all None values from dictionaries and lists, and returns
#     the result as a new dictionary or list.
#     """
#     if isinstance(value, list):
#         return [clean_nones(x) for x in value if x is not None]
#     elif isinstance(value, dict):
#         return {
#             key: clean_nones(val)
#             for key, val in value.items()
#             if val is not None
#         }
#     else:
#         return value

# split_point = int(pl_portion.shape[0]/6)

# pl_portion1 = pl_portion.slice(0, split_point)
# pl_portion2 = pl_portion.slice(split_point, split_point)
# pl_portion3 = pl_portion.slice(2*split_point, split_point)
# pl_portion4 = pl_portion.slice(3*split_point, split_point)
# pl_portion5 = pl_portion.slice(4*split_point, split_point)
# pl_portion6 = pl_portion.slice(5*split_point, split_point)

# json_format1 = "{\"MineralSite\":" + pl_portion1.write_json(pretty=True, row_oriented=True) + "}"
# json_item1 = loads(json_format1)
# json_item1 = clean_nones(json_item1)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/MRDS_1.json', 'w') as f:
#     dump(json_item1, f, indent=4, default=str)


# json_format2 = "{\"MineralSite\":" + pl_portion2.write_json(pretty=True, row_oriented=True) + "}"
# json_item2 = loads(json_format2)
# json_item2 = clean_nones(json_item2)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/MRDS_2.json', 'w') as f:
#     dump(json_item2, f, indent=4, default=str)

# json_format2 = "{\"MineralSite\":" + pl_portion3.write_json(pretty=True, row_oriented=True) + "}"
# json_item2 = loads(json_format2)
# json_item2 = clean_nones(json_item2)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/MRDS_3.json', 'w') as f:
#     dump(json_item2, f, indent=4, default=str)

# json_format2 = "{\"MineralSite\":" + pl_portion4.write_json(pretty=True, row_oriented=True) + "}"
# json_item2 = loads(json_format2)
# json_item2 = clean_nones(json_item2)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/MRDS_4.json', 'w') as f:
#     dump(json_item2, f, indent=4, default=str)

# json_format2 = "{\"MineralSite\":" + pl_portion5.write_json(pretty=True, row_oriented=True) + "}"
# json_item2 = loads(json_format2)
# json_item2 = clean_nones(json_item2)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/MRDS_5.json', 'w') as f:
#     dump(json_item2, f, indent=4, default=str)

# json_format2 = "{\"MineralSite\":" + pl_portion6.write_json(pretty=True, row_oriented=True) + "}"
# json_item2 = loads(json_format2)
# json_item2 = clean_nones(json_item2)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/MRDS_6.json', 'w') as f:
#     dump(json_item2, f, indent=4, default=str)

import polars as pl
minmod_commodities = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/minmod_commodities.csv')
MRDS_codes = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/MRDS_commodity_code.csv')
MRDS_codes = MRDS_codes.rename({
    'Commodity name': 'CommodityinMRDS',
    'Code': 'CodeinMRDS'
})

with_codes = pl.concat(
    [minmod_commodities, MRDS_codes],
    how='align'
)

# with_codes = with_codes.filter(
#     ~pl.col('minmod_id').is_null()
# )

with_codes.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/commodities_map.csv')

print(with_codes)

# print(minmod_commodities['CommodityinMRDS'].to_list())
# print(MRDS_codes)