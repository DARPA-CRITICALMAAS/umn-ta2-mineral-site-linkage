# from math import radians

# import polars as pl
# import geopandas as gpd
# from shapely import wkt

# from utils.convert_dataframe import *
# from sklearn.metrics.pairwise import haversine_distances

# def calculate_haversine(point_1:str, point_2:str):
#     point_1 = wkt.loads(point_1)
#     point_2 = wkt.loads(point_2)
#     if point_1.geom_type == 'MultiPoint':
#         point_1 = point_1.centroid

#     if point_2.geom_type == 'MultiPoint':
#         point_2 = point_2.centroid

#     location_1 = [point_1.x, point_1.y]
#     location_2 = [point_2.x, point_2.y]

#     location_1_rad = [radians(_) for _ in location_1]
#     location_2_rad = [radians(_) for _ in location_2]

#     value = haversine_distances([location_1_rad, location_2_rad]) * 6371000/1000

#     return value[0][1]

# pl_data = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/tungsten_grouped.csv')

# gpd_output = to_geopandas(pl_data, 'pl', geometry_column='location')

# gpd_series = gpd_output.dissolve(
#     'GroupID'
# ).convex_hull.centroid

# gpd_output = gpd.GeoDataFrame(geometry=gpd_series)
# pd_output = pd.DataFrame(gpd_output)
# pd_output = pd_output.reset_index()
# pd_output['geometry'] = pd_output['geometry'].apply(lambda x: wkt.dumps(x))

# groupID = pd_output['GroupID'].tolist()
# centroid = pd_output['geometry'].tolist()

# dict_mapping = dict(zip(groupID, centroid))

# pl_data = pl_data.with_columns(
#     centroid = pl.col('GroupID').replace(dict_mapping)
# ).with_columns(
#     distance = pl.struct(pl.all()).map_elements(lambda x: calculate_haversine(x['centroid'], x['location']))
# )

# pl_data = pl_data.filter(
#     pl.col('distance') > 0
# ).select(
#     pl.col(['GroupID', 'country', 'state_or_province', 'location', 'site_name', 'distance'])
# ).sort(by='distance')

# pl_data.write_csv('./distance_check.csv')

# pl_data = pl.read_excel('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/base_metal_deposit_compilation_hoggardetal2020.xls')

# print(pl_data)

import polars as pl
import pickle

# pl_data = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/intermediate.csv')
# print(set(pl_data['link_method'].to_list()))

dict_kg_map = {
    'names': 'site_name',
    'country': 'country',
    'province':'state_or_province',
    'centroid_epsg_4326': 'location',
}

with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/dict_kg_map.pkl', 'wb') as handle:
    pickle.dump(dict_kg_map, handle, protocol=pickle.HIGHEST_PROTOCOL)