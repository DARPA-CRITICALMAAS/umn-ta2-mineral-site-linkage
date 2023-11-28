import pandas as pd

from sklearn.cluster import DBSCAN, HDBSCAN, Birch

from minelink.m1_PreProcessing.dataframe_preprocessing import *
# from dataframe_formatting import *

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

    return df_loc_linked

def intra_linking():
    # tmp = pd.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/data/dict_Taylor.csv')
    # separate_dataframe(tmp)

    with open('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl/testing/MRDS_GBW.pkl', 'rb') as handle:
        df = pickle.load(handle)

    df_linked = location_based_linking(df)