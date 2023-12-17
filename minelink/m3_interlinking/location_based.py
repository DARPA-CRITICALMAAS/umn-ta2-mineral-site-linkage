import pandas as pd
import polars as pl
from sklearn.cluster import HDBSCAN

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

def location_based_linking(pl_data, bool_location):
    if bool_location:
        epsilon = 0.005
    else:
        epsilon = 0.05

    coords = pl_data[['long_rep', 'lat_rep']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)

    # series_labels = pl.DataFrame(clusters.labels_).rename('GroupID')

    pl_loc_linked = pl_data.select(
        idx = pl.col('intra_GroupID'),
        GroupID = pl.Series(clusters.labels_),
    )

    # df_loc_linked = pd.concat([df_geom, series_labels], axis=1)
    # pl_loc_linked = pl.from_pandas(df_loc_linked.drop('geometry', axis=1))

    # pl_loc_linked = pl_loc_linked.group_by(
    #     'GroupID'
    # ).agg(
    #     [pl.col('idx')]
    # ).sort('GroupID')

    # pl_loc_linked = pl_loc_linked.sort('idx')

    return pl_loc_linked