import os
import time
import logging
import configparser

import statistics
import polars as pl
import geopandas as gpd

from utils.convert_dataframe import *

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['geolocation.params']

def create_coordinate_point_representation(gpd_data):
    # Convert geometry to single point by finding centroid if the geometry is not a point
    # TODO: check that it is a centroid
    gpd_data['location'] = gpd_data['location'].apply(lambda x: x.centroid if x.type!='Point' else x)

    # Create a longitude, latitude column
    gpd_data['longitude'] = gpd_data.location.x
    gpd_data['latitude'] = gpd_data.location.y
    crs_value = geo_params['DEFAULT_CRS_SYSTEM']

    pl_data = to_polars(gpd_data, 'gpd')

    # aggregate the data based on the GroupID (create a list of latitudes and longitudes under the same group)
    pl_data = pl_data.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    )

    # Find the mean value of the latitudes and longitudes under the same group
    pl_data = pl_data.with_columns(
        pl.col('latitude').list.mean(),
        pl.col('longitude').list.mean(),
        pl.exclude(['latitude', 'longitude', 'GroupID']).list.unique().list.join(',')
    ).drop('GroupID')

    # convert back to geodataframe using the average longitude and latitude value
    gpd_data = to_geopandas(pl_data, 'pl', ['longitude', 'latitude'])

    return gpd_data

def create_buffer_area_representation(gpd_data):
    # Convert to metric system
    gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])
    
    # Dissolving based on GroupID and creating an integer convex hull around it
    gpd_polygon = gpd_data.dissolve(
        'GroupID'
    ).convex_hull.buffer(
        int(geo_params['POLYGON_BUFFER_unit_meter'])
    )

    gpd_polygon = gpd_polygon.to_frame().reset_index().sort_values(by=['GroupID']).rename_geometry('location')
    pl_data = to_polars(gpd_data, 'gpd').group_by(
        'GroupID'
    ).agg([pl.all()]).with_columns(
        pl.exclude(['latitude', 'longitude', 'GroupID']).list.unique().list.join(',')
    )

    pd_data = to_pandas(pl_data, 'pl')
    gpd_polygon = gpd_polygon.to_crs(crs=geo_params['DEFAULT_CRS_SYSTEM'])
    gpd_polygon = gpd_polygon.merge(pd_data, on='GroupID')

    return gpd_polygon