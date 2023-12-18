import pandas as pd
import polars as pl
import geopandas as gpd

from minelink.m0_load_input.save_ckpt import *
from minelink.m0_load_input.load_data import load_file
from minelink.m1_input_preprocessing.identify_columns import identify_column

def convert_df_to_dict(df_data):
    df_data = df_data.set_index('idx')
    dict_data = df_data.to_dict('index')

    return dict_data

def create_geometry_df(pl_data, col_latitude, col_longitude, crs_value):
    df_idx = pl_data.select(
        pl.col('idx')
    ).to_pandas()

    rep_latitude = col_latitude[0]
    rep_longitude = col_longitude[0]

    gpd_geom = gpd.GeoDataFrame(
        df_idx, geometry = gpd.points_from_xy(pl_data[rep_longitude], pl_data[rep_latitude], crs=crs_value)
    )

    pl_geom = pl_data.select(
        idx = pl.col('idx'),
        latitude = pl.col(rep_latitude),
        longitude = pl.col(rep_longitude)
    )

    return gpd_geom, pl_geom

def create_basic_info(pl_data, col_name):
    df_info = pl_data.select(
        pl.col(['idx', 'source_id', 'record_id']),
        name = pl.col(col_name),
    ).to_pandas()

    dict_basic_info = convert_df_to_dict(df_info)

    return dict_basic_info

def create_location_info(pl_data, dict_text_loc, crs_value, gpd_geom):
    pl_textual_data = pl_data.select(
        pl.col(list(dict_text_loc.keys())),
        crs = pl.lit(crs_value)
    ).rename(dict_text_loc).to_pandas()

    gpd_loc_info = pd.concat([gpd_geom, pl_textual_data], axis=1)
    dict_location_info = convert_df_to_dict(gpd_loc_info)

    return dict_location_info

def create_sameas(pl_data):
    pl_sameas = pl_data.select(
        pl.col(['idx', 'source_id', 'record_id'])
    ).to_pandas()

    dict_sameas = convert_df_to_dict(pl_sameas)

    return dict_sameas

def create_tolink_df(pl_data, remaining_columns):
    pl_tolink = pl_data.select(
        pl.col('idx'),
        pl.col(remaining_columns),
    )

    return pl_tolink

def process_dataframe(alias_code):
    alias_dict = load_file(list_path=[PATH_TMP_DIR],
                           file_name='alias_code',
                           file_extension='.pkl')
    
    pl_data = load_file(list_path=[PATH_TMP_DIR, alias_code], 
                        file_name='raw', 
                        file_extension='.pkl')
    pl_data = pl_data.drop('column_0')

    try:
        dict_data = load_file(list_path=[PATH_TMP_DIR, alias_code],
                              file_name='reg_dictionary',
                              file_extension='.pkl')

        unique_id, col_name, latitude, longitude, crs, dict_text_loc, remaining_columns = identify_column(pl_data, dict_data)
    except:
        unique_id, col_name, latitude, longitude, crs, dict_text_loc, remaining_columns = identify_column(pl_data)

    # Dropping original index column and adding our index columns
    if unique_id:
        pl_data = pl_data.with_columns(
            record_id = pl.col(unique_id)
        ).drop(unique_id)
    else:
        pl_data = pl_data.with_row_count(name='record_id', offset=1)

    pl_data = pl_data.with_columns(
        idx = pl.lit(alias_code) + '_' + pl.col('record_id').cast(pl.Utf8),
        source_id = pl.lit(alias_dict[alias_code]),
    )

    gpd_geom, pl_geom = create_geometry_df(pl_data, latitude, longitude, crs)
    save_ckpt(data=pl_geom, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='df_geometry')

    dict_basic_info = create_basic_info(pl_data, col_name)
    save_ckpt(data=dict_basic_info, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='basic_info')

    dict_location_info = create_location_info(pl_data, dict_text_loc, crs, gpd_geom)
    save_ckpt(data=dict_location_info, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='location_info')

    dict_sameas = create_sameas(pl_data)
    save_ckpt(data=dict_sameas, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='same_as')

    pl_tolink = create_tolink_df(pl_data, remaining_columns)
    save_ckpt(data=pl_tolink, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='df_tolink')