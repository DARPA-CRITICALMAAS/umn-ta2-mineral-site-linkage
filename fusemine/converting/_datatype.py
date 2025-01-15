from typing import Dict, List
from itertools import combinations, product

import polars as pl
import pandas as pd
import geopandas as gpd
from shapely import wkt

from ._crs import crs2crs

def geo2non(gpd_data: gpd.GeoDataFrame,
            geom_col: str='location') -> pl.DataFrame:
    """
    Converts geodataframe to polars data frame

    # TODO: fill in the information
    Arguments
    : gpd_data

    Return:
    """
    if 'crs' not in list(gpd_data.columns):
        gpd_data['crs'] = str(gpd_data.crs)
    
    gpd_data[geom_col] = gpd_data[geom_col].apply(wkt.dumps)
    # gpd_data = gpd_data.drop('geometry', axis=1)

    pl_data = pl.from_pandas(gpd_data)

    return pl_data

def non2geo(pl_data: pl.DataFrame,
            str_lat_col: str=None, str_long_col: str=None,
            str_geo_col: str=None,
            crs_val: str='EPSG:4326'):
    """
    # TODO: fill in information

    Argument
    : pl_data:
    : str_geo_col:
    : crs_val:

    Return
    : gpd_data
    : pl_data_nl
    """

    list_crs_partitioned_data = pl_data.partition_by('crs')

    list_gpd = []
    list_pl_data = []

    for d in list_crs_partitioned_data:
        sub_gpd_data, sub_pl_data_nl = crs2crs(input_data=d, input_crs=d.item(0, 'crs'), output_crs=crs_val)
        list_gpd.append(sub_gpd_data)
        list_pl_data.append(sub_pl_data_nl)

    gpd_data = gpd.GeoDataFrame(pd.concat(list_gpd, ignore_index=True), geometry=str_geo_col, crs=crs_val)
    pl_data_nl = pl.concat(
        list_pl_data,
        how='diagonal'
    )

    return gpd_data, pl_data_nl

def non2dict(pl_data: pl.DataFrame,
             key_col:str=None,
             val_col:str=None) -> Dict[str, str]:
    """
    Converts polars dataframe to dictionary. Each row is a key, val

    # TODO: fill in information
    Argument
    : pl_data:
    : key_col:

    Return
    """
    if not key_col and not val_col:
        raise ValueError("Need to state either key column or value column")
    
    if key_col and val_col:
        return dict(zip(pl_data[key_col].to_list(), pl_data[val_col].to_list()))

    if key_col:
        dict_data = pl_data.rows_by_key(key=key_col)
        dict_data = {k: v[0][0] for k, v in dict_data.items()}

    if val_col:
        key_cols = list(pl_data.columns)
        key_cols.remove(val_col)

        dict_data = {}

        for k in key_cols:
            pl_tmp = pl_data.select(pl.col([val_col, k])).drop_nulls(subset=[k])
            tmp_dict_data = pl_tmp.rows_by_key(key=k)
            tmp_dict_data = {k: v[0][0] for k, v in tmp_dict_data.items()}

            dict_data.update(tmp_dict_data)  

    return dict_data

def listlist2pairs(list_uris: List[List[str]]):
    list_pairs = []

    for i in list_uris:
        if isinstance(i[0], str):
            list_pairs.extend(list(combinations(i, 2)))
        elif isinstance(i[0], list):
            list_pairs.extend(list(product(i[0], i[1])))

    return list_pairs