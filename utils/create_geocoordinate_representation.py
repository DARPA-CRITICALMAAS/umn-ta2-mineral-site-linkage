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

def unify_crs(pl_data, crs_column=str):
    list_crs_separated_data = pl_data.partition_by(
        crs_column
    )

    for d in list_crs_separated_data:
        if d.item(0, 'crs') == geo_params['DEFAULT_CRS_SYSTEM']:
            continue

        gpd_data = to_geopandas(d, 'pl', 'location').to_crs(geo_params['DEFAULT_CRS_SYSTEM'])
        d = to_polars(gpd_data, 'gpd')

    pl_data = pl.concat(
        list_crs_separated_data,
        how='vertical_relaxed'
    )

    return pl_data

def create_coordinate_point_representation(gpd_data):
    # Convert geometry to single point by finding centroid if the geometry is not a point
    gpd_data['geometry'] = gpd_data['geometry'].apply(lambda x: x.centroid if x.type!='Point' else x)

    # Create a longitude, latitude column
    gpd_data['longitude'] = gpd_data.geometry.x
    gpd_data['latitude'] = gpd_data.geometry.y
    crs_value = geo_params['DEFAULT_CRS_SYSTEM']

    # aggregate the data based on the GroupID (create a list of latitudes and longitudes under the same group)
    gpd_data = gpd_data.drop(
        'geometry', axis=1
    ).groupby(
        'GroupID', axis=1
    ).agg(
        lambda x: [x]
    )

    # Find the mean value of the latitudes and longitudes under the same group
    gpd_data['longitude'] = gpd_data['longitude'].apply(lambda x: statistics.fmean(x))
    gpd_data['latitude'] = gpd_data['latitude'].apply(lambda x: statistics.fmean(x))

    # convert back to geodataframe using the average longitude and latitude value
    gpd_data = gpd.GeoDataFrame(
        gpd_data, geometry = gpd.points_from_xy(gpd_data['longitude'], gpd_data['latitude'], crs=crs_value)
    ).drop(
        ['longitude', 'latitude'], axis=1
    ).rename_geometry('location')

    return gpd_data

def create_buffer_area_representation(gpd_data):
    # Convert to metric system
    gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])
    
    # Dissolving based on GroupID and creatting an integer convex hull around it
    gpd_data = gpd_data.dissolve(
        'GroupID'
    ).convex_hull.buffer(
        int(geo_params['POLYGON_BUFFER_unit_meter'])
    )

    gpd_data = gpd_data.to_frame().reset_index().sort_values(by=['GroupID'])
    gpd_data = gpd_data.set_geometry('geometry')

    # Converting back to decimal system
    gpd_data = gpd_data.to_crs(crs=geo_params['DEFAULT_CRS_SYSTEM'])

    return gpd_data