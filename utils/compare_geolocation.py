import os
import time
import logging
import configparser

import statistics
import pandas as pd
import polars as pl
import geopandas as gpd
from shapely import wkt
from sklearn.cluster import HDBSCAN

from utils.convert_dataframe import *
from utils.dataframe_operations import *

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['geolocation.params']

def create_point_representation(gpd_data):
    gpd_data['longitude'] = gpd_data.geometry.x
    gpd_data['latitude'] = gpd_data.geometry.y
    crs_value = gpd_data['crs'][0]      # TODO: Need to check if it gives correct thing

    gpd_data = gpd_data.drop(
        'geometry', axis=1
    ).groupby(
        'GroupID', axis=1
    ).agg(
        lambda x: [x]
    )

    gpd_data['longitude'] = gpd_data['longitude'].apply(lambda x: statistics.fmean(x))
    gpd_data['latitude'] = gpd_data['latitude'].apply(lambda x: statistics.fmean(x))

    gpd_data = gpd.GeoDataFrame(
        gpd_data, geometry = gpd.points_from_xy(gpd_data['longitude'], gpd_data['latitude'], crs=crs_value)
    ).drop(
        ['longitude', 'latitude'], axis=1
    )

    return gpd_data

def compare_by_point(gpd_data):
    epsilon = float(geo_params['POINT_BUFFER_unit_meter'])

    gpd_data = gpd_data.to_crs(crs='EPSG:3857')

    # gpd_geolocationinfo = gpd.GeoDataFrame()
    # gpd_geolocationinfo['longitude'] = gpd_data.location.x
    # gpd_geolocationinfo['latitude'] = gpd_data.location.y

    # TODO: Need to check type and if polygon use centroid (or specify a point)
    coords = [gpd_data.location.x, gpd_data.location.y].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)
    list_cluster_labels = clusters.labels_

    gpd_data['GroupID'] = list_cluster_labels

    return gpd_data

def select_min_point_distance():
    return 0

def create_polygon_representation(gpd_data):
    gpd_data = gpd_data.to_crs(crs='EPSG:3857')
    
    gpd_polygon = gpd_data.dissolve(
        'GroupID'
    ).convex_hull.buffer(
        int(geo_params['POLYGON_BUFFER_unit_meter'])
    )

    gpd_polygon.name = 'location'
    gpd_polygon = gpd_polygon.to_frame().reset_index().sort_values(by=['GroupID'])
    gpd_polygon = gpd_polygon.set_geometry('location')

    return gpd_polygon

def compare_by_polygon(gpd_data, bool_select_max=False):
    minimum_overlap_threshold = float(geo_params['POLYGON_AREA_OVERLAP_unit_sqmeter'])

    gpd_overlapped_data = gpd.overlay(
        gpd_data, gpd_data,
        how='intersection', keep_geom_type=False
    )

    # TODO: Need to remove ones that are overlaying itself (A -> A)

    gpd_overlapped_data['overlapped_area'] = gpd_overlapped_data.area
    gpd_overlapped_data = gpd_overlapped_data.drop('geometry', axis=1)

    pl_overlapped_data = to_polars(gpd_overlapped_data, 'gpd')

    pl_overlapped_data = pl_overlapped_data.filter(
        pl.col('overlapped_area') > minimum_overlap_threshold
    ).sort(
        'overlapped_area'
    )

    if bool_select_max:
        pl_overlapped_data = select_max_overlapping_polygon(pl_overlapped_data) # TODO: Return as a polarws dataframe not a dictionary
    
    return dict(zip(pl_overlapped_data['GroupAlias_1'], pl_overlapped_data['GroupAlias_2']))

def select_max_overlapping_polygon(pl_polygon_region_data):
    pl_polygon_region_data = pl_polygon_region_data.unique(
        subset = ['GroupAlias_1'],
        maintain_order=True,
        keep='first'
    ).unique(
        subset = ['GroupAlias_2'],
        maintain_order=True,
        keep='first'
    )

    return pl_polygon_region_data

def compare_geolocation(list_pl_data, method:str|None=None, bool_select_max=False):
    if not method:
        return list_pl_data
    
    if bool_select_max and len(list_pl_data) > 1:
        pl_data = pl.concat(
            list_pl_data,
            how='vertical'
        )
        list_pl_data = [pl_data]
        del pl_data

    for pl_data in list_pl_data:
        gpd_data = to_geopandas(pl_data, 'pl', 'location')

        match method:
            case 'point':
                # TODO: Create method that will ensure all location are point
                if 'GroupID' in list(pl_data.columns):
                    gpd_data = create_point_representation(gpd_data)
                compare_by_point(gpd_data)

            case 'polygon':
                if 'GroupID' in list(pl_data.columns):
                    gpd_data = create_polygon_representation(gpd_data)
                pl_group_map = compare_by_polygon(gpd_data, bool_select_max)

                # map_values(pl_rawdata, pl_value_map, )
                           
            #    column_to_map: str, value_map_from: list|str, value_map_to: str, 
            #    prefix: str|None=None, store_original_value: str|None=None):

                pl_data = to_polars(gpd_data)
                pl_data = map_values(pl_rawdata=pl_data, pl_value_map=pl_group_map,
                                     column_to_map='GroupID', value_map_from='Group_Alias1', value_map_to='Group_Alias2',)

                pl_data = add_index_columns(pl_data = pl_data,
                                            index_column_name='GroupID',
                                            group_by_col='GroupID')

                # TODO: pass it through map values and add new index column