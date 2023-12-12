import polars as pl

def convert_df_to_dict(pl_data):
    df_data = pl_data.to_pandas()
    df_data = df_data.set_index('idx')
    dict_data = df_data.to_dict('index')

    return dict_data

def create_dict_info(pl_data, col_name):
    pl_info = pl_data.select(
        idx = pl.col('idx'),
        info = pl.col(col_name),
    )

    dict_info = convert_df_to_dict(pl_info)

    return dict_info

def create_dict_sameas(pl_data, source_name):
    pl_sameas = pl_data.select(
        pl.col('idx'),
        pl.col('source_id'),
        pl.col('record_id'),
    )

    dict_sameas = convert_df_to_dict(pl_sameas)

    return dict_sameas

def create_dict_geometry(pl_loc):
    pl_geo = pl_loc.select(
        idx = pl.col('idx'),
        geometry = pl.col('geometry'),
    )

    dict_geo = convert_df_to_dict(pl_geo)

    return dict_geo

def create_dict_location(pl_data, source_name):
    col_location = set(['geometry', 'country', 'state_or_province'])
    col_available = set(pl_data.columns)
    col_in_common = list(col_location & col_available)

    pl_loc = pl_data.select(
        pl.col(col_in_common),
        idx = pl.col('idx'),
    ).with_columns(
        crs = pl.lit('WGS84'),
    )

    # dict_geo = create_dict_geometry(pl_loc)
    dict_loc = convert_df_to_dict(pl_loc)

    return dict_loc #, dict_geo

def separate_dataframe(pl_data, source_alias_code, source_name):
    col_unique_id = False

    # col_unique_id, dict_loc_col_map, col_geocoordinates, col_sitename = find_columns(df, source_alias_code, source_name)

    if col_unique_id:
        pl_data = pl_data.with_columns(
            idx = source_alias_code + '_' + pl.col(col_unique_id).cast(pl.Utf8),
            record_id = pl.col(col_unique_id).cast(pl.Utf8),
            source_id = pl.lit(source_name)
        )
    else:
        pl_data = pl_data.with_row_count(name='record_id', offset=1).with_columns(
            idx = source_alias_code + '_' + pl.col('record_id').cast(pl.Utf8),
            source_id = pl.lit(source_name)
        )

    try:
        pl_geo = pl_data.select(
            idx = pl.col('idx'),
            geometry = pl.col('geometry'),
        )
    except:
        col_longitude = ['longitude']
        col_latitude = ['latitude']
        crs_val = 'WGS84'

        # if len(col_longitude)==1 and len(col_latitude)==1:
        #     df = convert_to_gdb(df, col_longitude[0], col_latitude[0], crs=crs_val)

    # try:
    #     df = unify_gdb_crs(df)
    # except:
    #     col_longitude = col_geocoordinates[0]
    #     col_latitude = col_geocoordinates[1]
    #     crs_val = col_geocoordinates[2]

    #     if len(col_longitude)==1 and len(col_latitude)==1:
    #         df = convert_to_gdb(df, col_longitude[0], col_latitude[0], crs=crs_val)

    #     else:
    #         precise_col_longitude = col_longitude[0]
    #         precise_col_latitude = col_latitude[0]

    #         df_len = pd.DataFrame()
    #         for i in col_longitude:
    #             df_len[i] = df[i].apply(lambda x: len(re.split('\.', x)[1]))

    #         df_len = df_len.max()

    #         for i in col_latitude:
    #             print()

    #         df = convert_to_gdb(df, precise_col_longitude, precise_col_latitude, crs=crs_val)

    # df = df.rename(columns=dict_loc_col_map)

    # # print(col_longitude, col_latitude, col_sitename)

    # dict_info = create_dict_info(df, col_sitename)
    # dict_sameas = create_dict_sameas(df, source_name)
    # dict_loc, dict_geo = create_dict_location(df, source_name)

    # return df_link

data_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/MRDS.csv'
df_csv = pl.read_csv(data_path)

separate_dataframe(df_csv, 'aa')