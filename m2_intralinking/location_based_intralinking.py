import os
import time
import logging
import configparser

import pandas as pd
import polars as pl
import geopandas as gpd
from sklearn.cluster import HDBSCAN, DBSCAN

config = configparser.ConfigParser()
config.read('../params.ini')

def geolocation_based_linking(pl_mineralsite):
    """
    Links records within a database based on the provided geolocation.

    : param: pl_mineralsite = 
    : return: pl_loc_linked_mineralsite = 
    """
    logging.info(f'\tInterlinking process started')
    start_time = time.time()

    # TODO: Need to check what the pl_mineralsite would look like and append the HDBSCAN portion
    epsilon = config['buffer.values']['INTRALINK_BUFFER']

    df_mineralsite = pl_mineralsite.to_pandas()

    gpd_geom = gpd.GeoDataFrame(
        df_mineralsite, geometry = gpd.points_from_xy(
            df_mineralsite['longitude'], 
            df_mineralsite['latitude'], 
            crs='WGS84')
    ).to_crs(epsg=3857)

    pl_loc_linked_mineralsite = df_mineralsite

    # Time logging
    logging.info(f'\tLocation based intralinking ended - Run Time: {time.time() - start_time}')

    return pl_loc_linked_mineralsite

# TODO
def deposit_based_linking(pl_mineralsite):
    """
    Links records within a database based on the deposit type information
    Runs iff geolocation is not available for the mineral site record

    : params: pl_mineralsite: 
    """

    return -1

def location_based_linking(pl_mineralsite):
    """
    Decides which method to use for location_based_linking.
    If there is geolocation information available by default it will perform geolocation based linking. Otherwise, it will be deposit based linking

    : param: pl_mineralsite = 
    """
    try:
        pl_mineralsite['location']
        pl_loc_linked_mineralsite = geolocation_based_linking(pl_mineralsite)
    except:
        pass

        # TODO: Will be available once deposit based linking function is completed
        # deposit_based_linking(pl_mineralsite)