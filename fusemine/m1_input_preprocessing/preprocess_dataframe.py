import pandas as pd
import polars as pl
import geopandas as gpd

from fusemine.m0_load_input.save_ckpt import *
from fusemine.m0_load_input.load_data import load_file
from fusemine.m1_input_preprocessing.identify_columns import identify_column

def convert_df_to_dict(df_data):
    df_data = df_data.set_index('idx')
    dict_data = df_data.to_dict('index')

    tmp_data = pd.DataFrame(dict_data)
    dict_data = {col: tmp_data[col].dropna().to_dict() for col in tmp_data}

    return dict_data

def gdb_to_geometry_df(geometry_data, crs_value):
    return 0

def create_geometry_df(pl_data, col_latitude, col_longitude, crs_value):
    df_idx = pl_data.select(
        pl.col('idx')
    ).to_pandas()

    rep_latitude = col_latitude[0]
    rep_longitude = col_longitude[0]

    gpd_geom = gpd.GeoDataFrame(
        df_idx, geometry = gpd.points_from_xy(pl_data[rep_longitude], pl_data[rep_latitude], crs=crs_value)
    ).rename_geometry(
        'location', inplace=False
    )

    pl_geom = pl_data.select(
        idx = pl.col('idx'),
        latitude = pl.col(rep_latitude),
        longitude = pl.col(rep_longitude)
    )

    return gpd_geom, pl_geom

def create_basic_info(pl_data, col_name):
    print("create basic info input", pl_data)
    print("name column input", col_name)

    df_info = pl_data.select(
        pl.col(['idx', 'source_id', 'record_id']),
        pl.col(col_name)
    ).to_pandas()

    # print("testing", df_info)

    # df_info = pl_data.select(
    #     pl.col(['idx', 'source_id', 'record_id']),
    #     name = pl.when(pl.col(col_name).str.strip_chars() == "")
    #         .then("Unknown")
    #         .otherwise(pl.col(col_name)).str.replace_all("[^\p{Ascii}]", ""),
    # ).to_pandas()

    dict_basic_info = convert_df_to_dict(df_info)

    return dict_basic_info

def create_location_info(pl_data, dict_text_loc, crs_value, gpd_geom):
    pl_textual_data = pl_data.select(
        pl.col(list(dict_text_loc.keys())),
        crs = pl.lit(crs_value)
    ).select(
        pl.when(pl.col(pl.Utf8).str.strip_chars() == "")
            .then(None)
            .otherwise(pl.col(pl.Utf8))
            .name.keep()
    ).rename(dict_text_loc).to_pandas()

    gpd_loc_info = pd.concat([gpd_geom, pl_textual_data], axis=1)

    print(gpd_loc_info)

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

    bool_gdb = False

    try:
        gpd_geom = pl_data.geometry
        crs_value = pl_data.crs.to_string()

        pd_data = pd.DataFrame(pl_data.drop(['geometry'], axis=1))
        pl_data = pl.from_pandas(pd_data)

        bool_gdb = True
    except:
        pass

    try:
        pl_data = pl_data.drop('column_0')
    except:
        pass

    
    dict_data = load_file(list_path=[PATH_TMP_DIR, alias_code],
                            file_name='reg_dictionary',
                            file_extension='.pkl')

    # identify_column(pl_data, dict_data)

    unique_id, col_name, latitude, longitude, crs, dict_text_loc, remaining_columns = identify_column(pl_data, dict_data)
    # except:
    #     # unique_id, col_name, latitude, longitude, crs, dict_text_loc, remaining_columns = identify_column(pl_data)
    #     pass

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

    print(pl_data)

    # if not bool_gdb:
    #     gpd_geom, pl_geom = create_geometry_df(pl_data, latitude, longitude, crs)
    #     save_ckpt(data=pl_geom, 
    #             list_path=[PATH_TMP_DIR, alias_code],
    #             file_name='df_geometry')
        
    gpd_geom, pl_geom = create_geometry_df(pl_data, latitude, longitude, crs)
    save_ckpt(data=pl_geom, 
            list_path=[PATH_TMP_DIR, alias_code],
            file_name='df_geometry')

    print("dataframe", pl_data)
    print("columns", col_name)

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