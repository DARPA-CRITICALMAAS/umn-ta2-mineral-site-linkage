import os
import time
import logging
import configparser
from tqdm import tqdm

import numpy as np
import statistics
import pandas as pd
import polars as pl
import geopandas as gpd
from shapely import wkt
from pyproj import CRS
from sklearn.cluster import HDBSCAN

from utils.convert_dataframe import *
from utils.dataframe_operations import *
from utils.create_geocoordinate_representation import *
from utils.unify_coordinate_system import *

from utils.save_files import *

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['geolocation.params']

def compare_point_distance(gpd_data, source_id:str,
                           epsilon=float(geo_params['POINT_BUFFER_UNIT_METER'])):
    # gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])

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
        how='diagonal_relaxed'
    ).rename(
        {'GroupID': 'GroupID_location'}
    )

    del pl_data_not_grouped
    return pl_data

def compare_buffer_overlap(gpd_data, source_id, 
                           minimum_overlap_threshold=float(geo_params['POLYGON_AREA_OVERLAP_UNIT_SQMETER'])):
    
    gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])

    gpd_overlapped_data = gpd.overlay(
        gpd_data, gpd_data,
        how='intersection',
        keep_geom_type=False,
    )    

    gpd_overlapped_union = gpd.overlay(
        gpd_data, gpd_data,
        how='union',
        keep_geom_type=False,
    )

    gpd_overlapped_data['intersection_area'] = gpd_overlapped_data['geometry'].area
    gpd_overlapped_data = gpd_overlapped_data[['GroupID_1', 'GroupID_2', 'intersection_area', 'source_id_1', 'source_id_2', 'record_id_1', 'record_id_2']]
    pl_intersection = to_polars(gpd_overlapped_data, 'gpd').with_columns(pl.exclude('intersection_area').cast(pl.Utf8))

    gpd_overlapped_union['union_area'] = gpd_overlapped_union['geometry'].area
    gpd_overlapped_union = gpd_overlapped_union[['GroupID_1', 'GroupID_2', 'union_area', 'source_id_1', 'source_id_2', 'record_id_1', 'record_id_2']]
    pl_union = to_polars(gpd_overlapped_union, 'gpd').with_columns(pl.exclude('union_area').cast(pl.Utf8))

    # pl_IOU = pl_intersection
    pl_IOU = pl.concat(
        [pl_intersection, pl_union],
        how='align'
    ).with_columns(
        IOU = pl.col('intersection_area') / pl.col('union_area')
    )

    pl_IOU = pl_intersection.filter(
        pl.col('GroupID_1') != pl.col('GroupID_2')
    )
    # pl_IOU.write_csv('./check.csv')

    if source_id == 'ALL':
        pl_IOU = pl_IOU.filter(
            pl.col('source_id_1') != pl.col('source_id_2')
        )

    # pl_IOU = pl_IOU.filter(
    #     pl.col('IOU') > minimum_overlap_threshold
    # )
    try:
        pl_IOU = pl_IOU.sort(
            'intersection_area'
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

        pl_data = to_polars(gpd_data, 'gpd').with_columns(
            new_GroupID = pl.col('GroupID')
        )

        for idx, row in enumerate(tqdm(pl_IOU.iter_rows(named=True))):
            group_id1 = pl_data.filter(
                pl.col('GroupID') == row['GroupID_1']
            ).item(0, 'new_GroupID')

            group_id2 = pl_data.filter(
                pl.col('GroupID') == row['GroupID_2']
            ).item(0, 'new_GroupID')

            pl_data = pl_data.with_columns(
                pl.col('new_GroupID').replace({group_id1:group_id2})
            )

        pl_data = pl_data.drop('GroupID').rename({
            'new_GroupID': 'GroupID_location'
        })
        
    except:
        pl_data = to_polars(gpd_data, 'gpd')
        pass

    # deleting for memory release
    del gpd_overlapped_data, pl_intersection
    # del gpd_overlapped_union, pl_union
    
    return pl_data

def compare_geolocation(pl_data, source_id:str|None=None, method:str|None=None):
    if not source_id:
        source_id = 'ALL'

    if 'GroupID' not in list(pl_data.columns):
        pl_data = add_index_columns(pl_data=pl_data,
                                    index_column_name='GroupID')
    
    pl_data = unify_crs(pl_data, crs_column='crs')

    if not method or pl_data.shape[0] < 2:
        pl_data = pl_data.with_columns(
            GroupID_location = pl.lit(source_id) + pl.lit('default')
        ).drop('GroupID')

        try:
            return to_polars(to_geopandas(pl_data, 'pl', 'location'), 'gpd')
        except:
            return pl_data
        
    pl_location_invalid = pl_data.filter(
        (pl.col('crs') == '') | (pl.col('location') == '')
    ).with_columns(
        GroupID_location = pl.lit(source_id) + pl.lit('default')
    ).drop('GroupID')

    pl_data = pl_data.filter(
        pl.col('crs') != ''
    )
    gpd_data = to_geopandas(pl_data, 'pl')
    
    start_time = time.time()
    match method:
        case 'distance':
            gpd_data = create_coordinate_point_representation(gpd_data)
            pl_data = compare_point_distance(gpd_data, source_id)
        
        case 'area':
            gpd_data = create_buffer_area_representation(gpd_data)
            pl_data = compare_buffer_overlap(gpd_data, source_id)

    pl_data = pl.concat(
        [pl_data, pl_location_invalid],
        how='diagonal_relaxed'
    )

    logging.info(f'\t\tLocation linking with {method} - Elapsed time: {time.time() - start_time}')

    return pl_data