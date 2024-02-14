from time import perf_counter

import pandas as pd
import polars as pl
import geopandas as gpd
from sklearn.cluster import HDBSCAN, DBSCAN

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

def location_based_linking(alias_code, bool_location):
    pl_geom = load_file([PATH_TMP_DIR, alias_code],
                        'df_geometry',
                        '.pkl')

    epsilon = INTRALINK_BOUNDARY

    df_geom = pl_geom.to_pandas()

    gpd_geom = gpd.GeoDataFrame(
        df_geom, geometry=gpd.points_from_xy(df_geom['longitude'], df_geom['latitude'], crs='WGS84')
    ).to_crs(epsg=3857)

    gpd_geom = gpd_geom.drop(['longitude', 'latitude'], axis=1)

    gpd_geom['longitude'] = gpd_geom.geometry.x
    gpd_geom['latitude'] = gpd_geom.geometry.y

    coords = pl_geom[['longitude', 'latitude']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)
    # clusters = DBSCAN(min_samples=2, eps=epsilon).fit(coords)

    # series_labels = pl.DataFrame(clusters.labels_).rename('GroupID')

    pl_loc_linked = pl_geom.select(
        idx = pl.col('idx'),
        GroupID = pl.Series(clusters.labels_).cast(pl.Int64),
    ).sort('idx')

    # df_loc_linked = pd.concat([df_geom, series_labels], axis=1)
    # pl_loc_linked = pl.from_pandas(df_loc_linked.drop('geometry', axis=1))

    # pl_loc_linked = pl_loc_linked.group_by(
    #     'GroupID'
    # ).agg(
    #     [pl.col('idx')]
    # ).sort('GroupID')

    # pl_loc_linked = pl_loc_linked.sort('idx')

    return pl_loc_linked