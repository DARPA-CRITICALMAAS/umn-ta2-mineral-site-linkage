import pandas as pd
from sklearn.cluster import DBSCAN, HDBSCAN, Birch

def link_with_loc(df_tolink):
    df_tolink['idx'] = df_tolink.index.astype(str)
    df_tolink = df_tolink.reset_index(drop=True)
    df_geometry = pd.concat([df_tolink['geometry'].x, df_tolink['geometry'].y], axis=1)
    df_geometry.columns = ['X', 'Y']
    coords = df_geometry[['X', 'Y']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=0.05).fit(coords)

    series_labels = pd.Series(clusters.labels_).rename('GroupID')

    df_linked = pd.concat([df_tolink, series_labels], axis=1)

    return df_linked