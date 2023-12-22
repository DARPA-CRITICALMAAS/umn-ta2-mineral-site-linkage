import polars as pl
import geopandas as gpd
from sklearn.cluster import HDBSCAN

from minelink.m0_load_input.load_data import load_file

def define_centroid(pl_linked_data, pl_geom):
    pl_linked_data = pl_linked_data.sort('idx')
    pl_geom = pl_geom.sort('idx').drop('idx')
    pl_linked_data = pl.concat([pl_linked_data, pl_geom], how='horizontal')

    pl_individual = pl_linked_data.filter(
        pl.col('GroupID') == -1
    )

    pl_group = pl_linked_data.filter(
        pl.col('GroupID') != -1
    )

    pl_group_loc_rep = pl_group.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    ).select(
        intra_GroupID = pl.col('GroupID'),
        lat_rep = pl.col('latitude').list.mean(),
        long_rep = pl.col('longitude').list.mean(),
    )

    pl_group_loc_rep = pl.concat(
        [pl_individual, pl_group_loc_rep],
        how='vertical'
    )

    return pl_group_loc_rep

def cluster_centroid(pl_data, bool_location):
    if bool_location:
        epsilon = 0.005
    else:
        epsilon = 0.05

    coords = pl_data[['longitude', 'latitude']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)

    pl_loc_linked = pl_data.select(
        idx = pl.col('idx'),
        GroupID = pl.Series(clusters.labels_),
    )

    return pl_loc_linked

def select_one_match():
    return 0

def location_based_linking(pl_data, bool_location):
    cluster_centroid(pl_data, bool_location)