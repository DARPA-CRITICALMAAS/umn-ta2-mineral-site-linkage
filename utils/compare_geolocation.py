import os
import time
import logging
import configparser

import numpy as np
import statistics
import pandas as pd
import polars as pl
import geopandas as gpd
from shapely import wkt
from sklearn.cluster import HDBSCAN

from utils.convert_dataframe import *
from utils.dataframe_operations import *
from utils.create_geocoordinate_representation import *
from utils.unify_coordinate_system import *

###
from utils.save_files import *
###

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['geolocation.params']

# TODO: function name so that it sounds like
def compare_point_distance(gpd_data, 
                           epsilon=float(geo_params['POINT_BUFFER_unit_meter'])):
    gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])

    coords = np.array(list(zip(gpd_data.location.x, gpd_data.location.y)))
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)
    list_cluster_labels = clusters.labels_

    gpd_data['GroupID'] = list_cluster_labels

    # Converting back to original CRS system
    gpd_data = gpd_data.to_crs(crs=geo_params['DEFAULT_CRS_SYSTEM'])
    pl_data = to_polars(gpd_data, 'gpd')
    total_data_length = pl_data.shape[0]

    pl_data_not_grouped = pl_data.filter(
        pl.col('GroupID') == -1
    ).drop('GroupID')
    not_grouped_length = pl_data_not_grouped.shape[0]
    
    pl_data_not_grouped = pl_data_not_grouped.with_columns(
        GroupID = pl.Series(list(range(total_data_length, total_data_length+not_grouped_length)))
    )

    pl_data = pl_data.filter(
        pl.col('GroupID') != -1
    )

    pl_data = pl.concat(
        [pl_data, pl_data_not_grouped],
        how='diagonal'
    )

    del pl_data_not_grouped
    return pl_data

def compare_buffer_overlap(gpd_data, source_id, 
                           minimum_overlap_threshold=float(geo_params['POLYGON_AREA_OVERLAP_unit_sqmeter'])):
    
    gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])

    gpd_data[['GroupID', 'ms_uri', 'location', 'source_id']].to_file('./raw.geojson', driver='GeoJSON')

    gpd_overlapped_data = gpd_data.overlay(
        gpd_data,
        how='intersection', keep_geom_type=False
    )
    gpd_overlapped_union = gpd_data.overlay(
        gpd_data,
        how='union', keep_geom_type=False
    )

    gpd_overlapped_data['intersection_area'] = gpd_overlapped_data.area
    gpd_overlapped_data = gpd_overlapped_data[['GroupID_1', 'GroupID_2', 'intersection_area', 'source_id_1', 'source_id_2']]
    pl_intersection = to_polars(gpd_overlapped_data, 'gpd')

    gpd_overlapped_union['union_area'] = gpd_overlapped_union.area
    gpd_overlapped_union = gpd_overlapped_union[['GroupID_1', 'GroupID_2', 'union_area', 'source_id_1', 'source_id_2']]
    pl_union = to_polars(gpd_overlapped_union, 'gpd')

    pl_IOU = pl.concat(
        [pl_intersection, pl_union],
        how='align'
    ).with_columns(
        IOU = pl.col('intersection_area') / pl.col('union_area')
    ).filter(
        pl.col('GroupID_1') != pl.col('GroupID_2')
    )

    if source_id == 'ALL':
        pl_IOU = pl_IOU.filter(
            pl.col('source_id_1') != pl.col('source_id_2')
        )

    pl_IOU = pl_IOU.filter(
        pl.col('IOU') > minimum_overlap_threshold
    ).sort(
        'IOU'
    ).with_columns(
        grouping = pl.concat_list(pl.col(['GroupID_1', 'GroupID_2'])).list.sort().list.to_struct()
    ).unnest('grouping').drop(['GroupID_1', 'GroupID_2']).rename(
        {
            'field_0': 'GroupID_1',
            'field_1': 'GroupID_2',
        }
    ).unique(
        subset=['GroupID_1', 'GroupID_2'],
        maintain_order=True,
        keep='first'
    ).unique(
        subset=['GroupID_1', 'source_id_1', 'source_id_2'],
        maintain_order=True,
        keep='first'
    ).unique(
        subset=['GroupID_2', 'source_id_1', 'source_id_2'],
        maintain_order=True,
        keep='first'
    )

    # deleting for memory release
    del gpd_overlapped_data, pl_intersection, gpd_overlapped_union, pl_union

    # pl_data = to_polars(gpd_data, 'gpd')

    # print(pl_data.columns)

    pl_data = map_values(to_polars(gpd_data, 'gpd'), pl_IOU, 
                         column_to_map='GroupID', value_map_from='GroupID_1', value_map_to='GroupID_2')

    return pl_data

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

def compare_geolocation(pl_data, source_id:str|None=None, method:str|None=None):
    if not source_id:
        source_id = 'ALL'

    pl_data = unify_crs(pl_data, crs_column='crs')
    if 'GroupID' not in list(pl_data.columns):
        pl_data = add_index_columns(pl_data=pl_data,
                                    index_column_name='GroupID')
        
    if not method or pl_data.shape[0] < 2:
        pl_data = pl_data.with_columns(
            GroupID_location = pl.lit(source_id) + pl.col('GroupID').cast(pl.Utf8)
        ).drop('GroupID')

        return to_polars(to_geopandas(pl_data, 'pl', 'location'), 'gpd')
        
    try:
        gpd_data = to_geopandas(pl_data, 'pl', 'location')
    except:
        gpd_data = to_geopandas(pl_data, 'pl', ['longitude', 'latitude'])

    logging.info(f'\t\tlocation linking with: {method}')
    
    match method:
        case 'distance':
            gpd_data = create_coordinate_point_representation(gpd_data)
            pl_data = compare_point_distance(gpd_data)
        
        case 'area':                
            gpd_data = create_buffer_area_representation(gpd_data)   
            pl_data = compare_buffer_overlap(gpd_data, source_id)

    pl_data = pl_data.with_columns(
        GroupID_location = pl.lit(source_id) + pl.col('GroupID').cast(pl.Utf8)
    ).drop('GroupID')

    return pl_data