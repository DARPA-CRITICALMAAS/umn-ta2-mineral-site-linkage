import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from minelink.params import *
from minelink.m0_SaveAndLoad.save_load_file import *
from minelink.m5_PostProcessing.determine_location import *

def create_output_dataframe(df_linked, dict_info, dict_location, dict_sameas):
    df_output = df_linked

    # based on idx in df_linked call dict info, dict location, an dict sameas
    # for dict location, create a dataframe and select centroid

    return df_linked

def postprocessing(df_linked, dict_info, dict_location, dict_sameas, dict_geom):
    dict_code_alias = load_file(PATH_TMP_DIR, 'code_alias', '.pkl')
    
    df_linked_group = df_linked[df_linked.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))
    df_linked_indiv = df_linked[df_linked.GroupID==-1].drop('GroupID', axis=1)

    return 0

# def create_sameas(list_idx, list_source, dict_sameas, dict_geo):
#     compiled_sameas = {key:[] for key in list_source}

#     for i in list_idx:
#         source = str(regex.split('\d', i)[0])
#         source_id = int(regex.findall('\d+', i)[0])
#         geom_data = dict_geo[i]['geometry']

#         row_dict = {
#             "id": source_id,
#             "Attributes": dict_sameas[i],
#             "geometry": geom_data,
#         }

#         compiled_sameas[source].append(row_dict)

#     return compiled_sameas


# def convert_to_output_form(df_site, dict_location, dict_sameas, dict_geo):
#     df_site_w = df_site[df_site.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))

#     df_site_i = df_site[df_site.GroupID == -1].drop('GroupID', axis=1)
#     df_site_i['site_name'] = df_site_i['site_name'].apply(lambda x: [x])
#     df_site_i['idx'] = df_site_i['idx'].apply(lambda x: [x])
#     df_site_i['location_source'] = df_site_i['location_source'].apply(lambda x: [x])

#     df_site = pd.concat([df_site_w, df_site_i], ignore_index=True, axis=0)

#     df_site['id'] = 'Site' + df_site.index.astype(str)
#     df_site = df_site.set_index('id')

#     df_site['name'] = df_site['site_name'].apply(lambda x: x[0][0])

#     df_site = df_site.drop('site_name', axis=1)
#     df_site['location_info'] = df_site['idx'].apply(lambda x: dict_location[x[0]])

#     df_site['same_as'] = df_site.apply(lambda x: create_sameas(x['idx'], x['location_source'], dict_sameas, dict_geo), axis=1)
#     df_site = df_site.drop(['location_source', 'idx'], axis=1).reset_index()
#     df_site = df_site[['id', 'name', 'location_info', 'same_as']]

#     return df_site