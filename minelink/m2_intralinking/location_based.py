import pandas as pd
import polars as pl
from sklearn.cluster import HDBSCAN

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

def location_based_linking(alias_code, bool_location):
    gpd_geom = load_file([PATH_TMP_DIR, alias_code], 
                         'df_geometry',
                         '.pkl')
    
    if bool_location:
        epsilon = 0.005
    else:
        epsilon = 0.05

    df_geometry = pd.concat([gpd_geom['geometry'].x, gpd_geom['geometry'].y], axis=1)
    df_geometry.columns = ['X', 'Y']
    coords = df_geometry[['X', 'Y']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)

    series_labels = pd.Series(clusters.labels_).rename('GroupID')

    df_loc_linked = pd.concat([gpd_geom, series_labels], axis=1)
    pl_loc_linked = pl.from_pandas(df_loc_linked.drop('geometry', axis=1))

    return pl_loc_linked