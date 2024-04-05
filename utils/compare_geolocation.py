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
from utils.create_geocoordinate_representation import *

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['geolocation.params']

def compare_point_distance(gpd_data):
    epsilon = float(geo_params['POINT_BUFFER_unit_meter'])
    gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])

    coords = [gpd_data.location.x, gpd_data.location.y].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)
    list_cluster_labels = clusters.labels_

    gpd_data['GroupID'] = list_cluster_labels

    # Converting back to original CRS system
    gpd_data = gpd_data.to_crs(crs=geo_params['DEFAULT_CRS_SYSTEM'])

    return gpd_data

def compare_buffer_overlap(gpd_data):
    minimum_overlap_threshold = float(geo_params['POLYGON_AREA_OVERLAP_unit_sqmeter'])

    gpd_overlapped_data = gpd.overlay(
        gpd_data, gpd_data,
        how='intersection', keep_geom_type=False
    )

    gpd_overlapped_union = gpd.overlay(
        gpd_overlapped_data, gpd_overlapped_data,
        how='union', keep_geom_type=False
    )

    gpd_overlapped_data['intersected_area'] = gpd_overlapped_data.area
    gpd_overlapped_data['union_area'] = gpd_overlapped_union.area
    gpd_overlapped_data['IOU'] = gpd_overlapped_data.apply(lambda x: x['intersected_area'] / x['union_area'], axis=1)
    
    gpd_overlapped_data = gpd_overlapped_data.drop(['geometry', 'intersected_area', 'union_area'], axis=1)
    pl_overlapped_data = to_polars(gpd_overlapped_data, 'gpd')

    pl_overlapped_data = pl_overlapped_data.filter(
        pl.col('IOU') > minimum_overlap_threshold, 
        pl.col('IOU') < 1,
    ).sort(
        'IOU'
    ).select(
        pl.col(['GroupID_1', 'GroupdID_2'])
    )

    pl_data = map_values(to_polars(gpd_data, 'gpd'), pl_overlapped_data, 
                         column_to_map='GroupID', value_map_from='GroupID_1', value_map_to='GroupID_2')
    
    return to_geopandas(pl_data, 'pl')

# def select_area_max_overlap(gpd_data):
#     pl_polygon_region_data = pl_polygon_region_data.unique(
#         subset = ['GroupAlias_1'],
#         maintain_order=True,
#         keep='first'
#     ).unique(
#         subset = ['GroupAlias_2'],
#         maintain_order=True,
#         keep='first'
#     )

#     return pl_polygon_region_data

def compare_geolocation(list_pl_data, method:str|None=None):
    if not method:
        return list_pl_data
    
    for pl_data in list_pl_data:
        pl_data = unify_crs(pl_data, crs_column=str)
        gpd_data = to_geopandas(pl_data, 'pl', 'location')

        if 'GroupID' not in list(pl_data.columns):
            pl_data = add_index_columns(pl_data=pl_data,
                                        index_column_name='GroupID')

        match method:
            case 'distance':
                # Ensures that all geometries are wkt.Point
                gpd_data = create_coordinate_point_representation(gpd_data)
                gpd_data = compare_point_distance(gpd_data)

            case 'area':
                # Creates a polygon geometry with the specified buffer for all geometry
                gpd_data = create_buffer_area_representation(gpd_data)
                gpd_data = compare_buffer_overlap(gpd_data)

        pl_data = to_polars(gpd_data)
        # pl_data = add_index_columns(pl_data=pl_data,
        #                             index_column_name='GroupID',
        #                             group_by_col='GroupID')
        
        pl_data = pl_data.group_by('GroupID').agg([pl.all()])

    return list_pl_data