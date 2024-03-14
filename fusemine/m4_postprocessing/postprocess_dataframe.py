import os
import logging
from time import perf_counter

import regex as re
import polars as pl
import pandas as pd
import geopandas as gpd

from fusemine.params import *
from fusemine.m0_load_input.load_data import load_file
from fusemine.m0_load_input.save_ckpt import open_ckpt_dir

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

def postprocess_toGeoJSON(bool_interlink):
    if bool_interlink:
        linked_file_name = 'pl_linkedZn'
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
    ).drop_nulls(['idx']).sort('idx')
    
    list_linked = pl_linked.with_columns(
        code_alias = pl.col('idx').str.split('_').list.get(0)
    ).partition_by('code_alias')

    # print(pl_linked)

    pl_geometry = pl.DataFrame()
    pl_name = pl.DataFrame()
    pl_commodity = pl.DataFrame()

    # counter = pl.DataFrame()

    for i in list_linked:
        alias_code = i.item(0, 'code_alias')

        tmp_geometry = load_file([PATH_TMP_DIR, alias_code], 'df_geometry', '.pkl')
        tmp_geometry = tmp_geometry.select(
            idx = pl.col('idx').cast(pl.Utf8),
            latitude = pl.col('latitude').cast(pl.Float64),
            longitude = pl.col('longitude').cast(pl.Float64)
        ).drop_nulls(['idx']).sort('idx')

        # print("geometry", tmp_geometry)

        # dict_locationinfo = load_file([PATH_TMP_DIR, alias_code], 'location_info', '.pkl')
        # tmp_locationinfo = pd.DataFrame.from_dict(dict_locationinfo, orient='index')
        # tmp_locationinfo.reset_index(inplace=True)

        # # print("basic info", tmp_basicinfo)
        # tmp_locationinfo = pl.from_pandas(tmp_locationinfo).select(
        #     idx = pl.col('index').cast(pl.Utf8),
        #     latitude = pl.col('latitude').cast(pl.Float64),
        #     longitude = pl.col('longitude').cast(pl.Float64),
        #     country = pl.col('country').cast(pl.Utf8)
        # ).drop_nulls(['idx']).sort('idx')

        # counter = pl.concat(
        #     [counter, tmp_locationinfo],
        #     how='vertical'
        # )

        dict_basicinfo = load_file([PATH_TMP_DIR, alias_code], 'basic_info', '.pkl')
        tmp_basicinfo = pd.DataFrame.from_dict(dict_basicinfo, orient='index')
        tmp_basicinfo.reset_index(inplace=True)

        # print("basic info", tmp_basicinfo)
        tmp_basicinfo = pl.from_pandas(tmp_basicinfo).select(
            idx = pl.col('index').cast(pl.Utf8),
            name = pl.col('name').cast(pl.Utf8),
            source = pl.col('source_id').cast(pl.Utf8),
            source_id = pl.col('record_id').cast(pl.Utf8),
        ).drop_nulls(['idx']).sort('idx')

        pl_otherinfo = load_file([PATH_TMP_DIR, alias_code], 'df_tolink', '.pkl')
        try:
            pl_otherinfo = pl_otherinfo.select(
                pl.col('idx'),
                pl.col(['commod1', 'commod2', 'commod3']),
            ).with_columns(
                commodity = (pl.col('commod1') + ', ' + pl.col('commod2') + ', ' + pl.col('commod3')).str.split(',')
            ).explode(
                'commodity'
            ).with_columns(
                pl.col('commodity').str.strip()
            ).drop(['commod1', 'commod2', 'commod3']).filter(
                pl.col('commodity') != ""
            ).group_by(
                pl.col('idx')
            ).agg(
                [pl.col('commodity')]
            ).with_columns(
                pl.col('commodity').list.join(", ")
            )

        except:
            try:
                pl_otherinfo = pl_otherinfo.select(
                    pl.col('idx'),
                    pl.col('commodity')
                )
            except:
                pl_otherinfo = pl_otherinfo.select(
                    pl.col('idx'),
                    commodity = pl.col('Commodity').str.replace_all(';', ',')
                )

        pl_otherinfo = pl_otherinfo.drop_nulls(['idx']).sort('idx')

        pl_name = pl.concat(
            [pl_name, tmp_basicinfo],
            how='vertical'
        )

        pl_geometry = pl.concat(
            [pl_geometry, tmp_geometry],
            how='vertical'
        )

        pl_commodity = pl.concat(
            [pl_commodity, pl_otherinfo],
            how='vertical'
        )

    pl_name = pl_name.sort('idx').drop_nulls(['idx']).drop('idx')
    pl_geometry = pl_geometry.sort('idx').drop_nulls(['idx']).drop('idx')
    pl_commodity = pl_commodity.sort('idx').drop_nulls(['idx']).drop('idx')

    df_tojson = pl.concat(
        [pl_linked, pl_geometry, pl_name, pl_commodity],
        how='horizontal'
    ).drop_nulls(['idx']).to_pandas()

    df_tojson['color_code'] = df_tojson['prediction'].apply(lambda x: x%23)

    gpd_geom = gpd.GeoDataFrame(
        df_tojson, geometry=gpd.points_from_xy(df_tojson['longitude'], df_tojson['latitude'], crs='WGS84')
    )

    return gpd_geom
