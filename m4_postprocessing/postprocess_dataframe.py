import os
import logging
from time import perf_counter

import regex as re
import polars as pl
import pandas as pd
import geopandas as gpd

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import open_ckpt_dir

def get_info(list_alias, list_idx, split_keyword):
    source_id = list_alias[0]
    basic_info = load_file([PATH_TMP_DIR, source_id], 'basic_info', '.pkl')
    location_info = load_file([PATH_TMP_DIR, source_id], 'location_info', '.pkl')

    rep_basic = basic_info[list_idx[0]]
    rep_location = location_info[list_idx[0]]

    same_as = []
    for i in list_idx:
        source_id, record_id = re.split(split_keyword, i)
        same_as_info = load_file([PATH_TMP_DIR, source_id], 'same_as', '.pkl')

        same_as.append(same_as_info[i])

    try:
        return rep_basic['name'], rep_basic['source_id'], rep_basic['record_id'], rep_location, same_as
    except:
        return 'Unnamed', rep_basic['source_id'], rep_basic['record_id'], rep_location, same_as
  

def postprocessing(bool_interlink):
    module_start = perf_counter()

    if bool_interlink:
        linked_file_name = 'pl_linkedNi'
        pl_linked = load_file([PATH_TMP_DIR], linked_file_name, '.pkl')
        split_keyword = '_'     # May need to be changed later
    else:
        linked_file_name = 'pl_intra_linked'
        pl_linked = load_file([PATH_TMP_DIR, 'aa'], linked_file_name, '.pkl')
        split_keyword = '_'
    
    dict_alias = load_file([PATH_TMP_DIR], 'alias_code', '.pkl')

    pl_output = pl_linked.filter(
        pl.col('GroupID') != -1
    ).with_columns(
        pl.col('idx').str.split(by='_').list.to_struct(n_field_strategy="max_width").alias('tmp_struct')
    ).unnest('tmp_struct').rename(
        {'field_0':'source_alias', 'field_1':'record_id'}
    ).group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    )
    # .map_rows(
    #     lambda x: get_info(x[1], split_keyword, dict_alias)
    # # ).rename(
    # #     {'column_0': 'name',
    # #      'column_1': 'source_id',
    # #      'column_2': 'record_id',}
    #     #  'column_3': 'location_info',
    #     #  'column_4': 'same_as'}
    # )

    # print(pl_output)

    # pl_output.write_json('./sample.json')

    df_output = pl_output.to_pandas()
    
    df_linked = pd.DataFrame()
    df_linked[['name', 'source_id', 'record_id', 'location_info', 'same_as']] = df_output.apply(lambda x: get_info(x['source_alias'], x['idx'], split_keyword), axis=1, result_type="expand")

    module_end = perf_counter()

    # df_output = pl_output.to_pandas()

    return df_linked

def get_geojson_info(list_alias, list_idx, split_keyword):
    source_id = list_alias[0]
    basic_info = load_file([PATH_TMP_DIR, source_id], 'basic_info', '.pkl')
    location_info = load_file([PATH_TMP_DIR, source_id], 'location_info', '.pkl')

    rep_basic = basic_info[list_idx[0]]
    rep_location = location_info[list_idx[0]]

    geometry = rep_location['location']

    rep_location['location'] = str(rep_location['location'])

    same_as = []
    for i in list_idx:
        source_id, record_id = re.split(split_keyword, i)
        same_as_info = load_file([PATH_TMP_DIR, source_id], 'same_as', '.pkl')

        same_as_string = same_as_info[i]['source_id'] + str(same_as_info[i]['record_id'])

        same_as.append(same_as_string)

    all_same_as = ", ".join(same_as)

    try:
        return rep_basic['name'], rep_basic['source_id'], rep_basic['record_id'], rep_location, all_same_as, geometry
    except:
        return 'Unnamed', rep_basic['source_id'], rep_basic['record_id'], rep_location, all_same_as, geometry

def postprocess_toGeoJSON(list_code, bool_interlink):
    if bool_interlink:
        linked_file_name = 'pl_linkedNi'
        pl_linked = load_file([PATH_TMP_DIR], linked_file_name, '.pkl')
        split_keyword = '_'     # May need to be changed later
    else:
        linked_file_name = 'pl_intra_linked'
        pl_linked = load_file([PATH_TMP_DIR, 'aa'], linked_file_name, '.pkl')
        split_keyword = '_'
    
    dict_alias = load_file([PATH_TMP_DIR], 'alias_code', '.pkl')

    pl_linked = pl_linked.select(
        idx = pl.col('idx'),
        prediction = pl.col('GroupID')
    ).sort('idx')

    # print(pl_linked)

    pl_geometry = pl.DataFrame()
    # pl_other_info = pl.DataFrame()
    pl_name = pl.DataFrame()

    for i in list_code:
        tmp_geometry = load_file([PATH_TMP_DIR, i], 'df_geometry', '.pkl')
        print("org geometry", tmp_geometry)
        tmp_geometry = tmp_geometry.select(
            idx = pl.col('idx').cast(pl.Utf8),
            latitude = pl.col('latitude').cast(pl.Float64),
            longitude = pl.col('longitude').cast(pl.Float64)
        )
        # tmp_other = load_file([PATH_TMP_DIR, i], 'df_tolink', '.pkl')
        dict_basicinfo = load_file([PATH_TMP_DIR, i], 'basic_info', '.pkl')

        tmp_basicinfo = pd.DataFrame.from_dict(dict_basicinfo, orient='index')
        tmp_basicinfo.reset_index(inplace=True)

        print("basic info", tmp_basicinfo)
        tmp_basicinfo = pl.from_pandas(tmp_basicinfo).select(
            idx = pl.col('index').cast(pl.Utf8),
            name = pl.col('name').cast(pl.Utf8),
            source = pl.col('source_id').cast(pl.Utf8),
            source_id = pl.col('record_id').cast(pl.Utf8),
        )

        print("geometry", tmp_geometry)
         
        pl_name = pl.concat(
            [pl_name, tmp_basicinfo],
            how='vertical'
        )

        pl_geometry = pl.concat(
            [pl_geometry, tmp_geometry],
            how='vertical'
        )

        # pl_other_info = pl.concat(
        #     [pl_other_info, tmp_other],
        #     how='diagonal'
        # )

    # print(pl_linked)

    pl_name = pl_name.sort('idx').drop('idx')
    pl_geometry = pl_geometry.sort('idx').drop('idx')
    # pl_other_info = pl_other_info.sort('idx').drop('idx')

    df_tojson = pl.concat(
        [pl_linked, pl_geometry, pl_name],
        how='horizontal'
    ).drop_nulls(['idx']).to_pandas()

    print(df_tojson.columns)

    gpd_geom = gpd.GeoDataFrame(
        df_tojson, geometry=gpd.points_from_xy(df_tojson['longitude'], df_tojson['latitude'], crs='WGS84')
    )

    return gpd_geom