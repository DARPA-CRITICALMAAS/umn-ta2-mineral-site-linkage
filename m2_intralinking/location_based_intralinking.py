import os
import time
import logging
import configparser

import pandas as pd
import polars as pl
import geopandas as gpd
import wkt
from sklearn.cluster import HDBSCAN, DBSCAN

config = configparser.ConfigParser()
config.read('../params.ini')

def geolocation_based_linking(gpd_mineralsite):
    """
    Links records within a database based on the provided geolocation.

    : param: gpd_mineralsite = 
    : return: pl_loc_linked_mineralsite = 
    """
    # TODO: Need to check what the pl_mineralsite would look like and append the HDBSCAN portion
    epsilon = config['buffer.values']['INTRALINK_BUFFER']

    gpd_mineralsite['longitude'] = gpd_mineralsite.location.x
    gpd_mineralsite['latitude'] = gpd_mineralsite.location.x

    coords = gpd_mineralsite[['longitude', 'latitude']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)
    list_cluster_labels = clusters.labels_

    pd_mineralsite = pd.DataFrame(gpd_mineralsite)
    pl_loc_linked_mineralsite = pl.from_pandas(pd_mineralsite).with_columns(
        GroupID = pl.Series(list_cluster_labels).cast(pl.Int64)
    )

    return pl_loc_linked_mineralsite

# TODO
def deposit_based_linking(pl_mineralsite):
    """
    Links records within a database based on the deposit type information
    Runs iff geolocation is not available for the mineral site record

    : params: pl_mineralsite: 
    """

    return -1

def location_based_linking(pl_preprocessed_mineralsite):
    """
    Decides which method to use for location_based_linking.
    If there is geolocation information available by default it will perform geolocation based linking. Otherwise, it will be deposit based linking

    : param: pl_mineralsite = 
    """
    try:
        df_preprocessed_mineralsite = pl_preprocessed_mineralsite.to_pandas()
        df_preprocessed_mineralsite['location'] = df_preprocessed_mineralsite['location'].apply(wkt.loads)

        gpd_mineralsite = gpd.GeoDataFrame(
            df_preprocessed_mineralsite, 
            geometry='location', 
            crs='WGS84'
        )

        pl_loc_linked_mineralsite = geolocation_based_linking(gpd_mineralsite)


    except:
        pass
        # TODO: Will be available once deposit based linking function is completed
        # deposit_based_linking(pl_mineralsite)