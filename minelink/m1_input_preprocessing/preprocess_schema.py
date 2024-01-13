import pandas as pd
import polars as pl
import geopandas as gpd

from minelink.m0_load_input.save_ckpt import *
from minelink.m0_load_input.load_data import load_file

def process_schema_data():
    # pl_data = pl.read_json()
    # query = ''' SELECT ?ms ?ms_p ?ms_v
    #         WHERE {
    #             ?ms a :MineralSite .
    #             ?ms ?ms_p ?ms_v .
    #         } '''
    # run_minmod_query(query, values=True)


# def process_dataframe(alias_code):
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

    if not bool_gdb:
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