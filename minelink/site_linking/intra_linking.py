import pandas as pd


from sklearn.cluster import DBSCAN, HDBSCAN, Birch

# from minelink.site_linking.dataframe_extraction import *
from dataframe_formatting import *

import pickle5 as pickle

def location_based_linking(df_tolink):
    df_tolink = df_tolink.reset_index(drop=True)
    df_geometry = pd.concat([df_tolink['geometry'].x, df_tolink['geometry'].y], axis=1)
    df_geometry.columns = ['X', 'Y']
    coords = df_geometry[['X', 'Y']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=0.05).fit(coords)

    series_labels = pd.Series(clusters.labels_).rename('GroupID')

    df_linked = pd.concat([df_tolink, series_labels], axis=1)

    return df_linked

def site_name_based_linking(df_tolink):
    df_loc_linked = location_based_linking(df_tolink)

    df_tolink = df_loc_linked

    return df_tolink

def intra_linking():
    # tmp = pd.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/data/dict_Taylor.csv')
    # separate_dataframe(tmp)

    with open('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl/testing/MRDS_GBW.pkl', 'rb') as handle:
        df = pickle.load(handle)

    df_linked = location_based_linking(df)


intra_linking()

# def intra_group_s(filename1, output_filename):
#     df_test1 = pickle_load('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl', filename1)
#     dict_loc, gpd_site, dict_sameas, dict_geo, col_available_loc = format_dataframe(df_test1, filename1)

#     gpd_site = location_group(gpd_site)
    
#     gpd_grouped = gpd_site.groupby(['GroupID']).agg(lambda x: list(x))

#     gpd_grouped['loc_group'] = gpd_grouped.index
#     gpd_grouped.reset_index(drop=True, inplace=True)

#     gpd_individual = gpd_grouped[gpd_grouped['loc_group'] == -1]
#     gpd_individual = gpd_individual.drop('loc_group', axis=1)
#     gpd_individual = gpd_individual.explode(list(gpd_individual.columns))
#     gpd_individual.insert(loc=0, column='GroupID', value=-1)
    
#     gpd_with_group = gpd_grouped[gpd_grouped['loc_group'] != -1]
#     gpd_with_group['NewGroupID'] = gpd_with_group.apply(lambda x: gen_new_group(x['loc_group'], gpd_grouped.shape[0], x['site_name']), axis=1)

#     gpd_with_group = gpd_with_group.drop('loc_group', axis=1)
#     gpd_with_group = gpd_with_group.explode(list(gpd_with_group.columns))
#     gpd_with_group = gpd_with_group.rename(columns={'NewGroupID': 'GroupID'})

#     df_site = pd.concat([gpd_individual, gpd_with_group], axis=0)
#     df_site.reset_index(drop=True, inplace=True)

#     df_final = convert_to_output_form(df_site, dict_loc, dict_sameas, dict_geo)
#     json_dump(df_final, './outputs', output_filename)

#     return reindex_indiv(df_site), dict_loc, dict_sameas, dict_geo