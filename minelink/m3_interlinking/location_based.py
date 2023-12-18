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

    pl_loc_linked = pl_data.select(
        idx = pl.col('intra_GroupID'),
        GroupID = pl.Series(clusters.labels_),
    )

    return pl_loc_linked