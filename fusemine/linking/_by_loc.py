import numpy as np
import polars as pl
import geopandas as gpd

from sklearn.cluster import HDBSCAN

def group_proximity(gpd_input: gpd.GeoDataFrame,
                    geom_col: str='location') -> gpd.GeoDataFrame:
    """
    TODO: need to fill

    Arguments:
    gpd_input: 
    """
    gpd_input = gpd_input.to_crs(crs='EPSG:4087')
    arr_coordinates = np.array(list(zip(gpd_input[geom_col].x, gpd_input[geom_col].y)))

    clusters = HDBSCAN(min_cluster_size=2,
                       cluster_selection_epsilon=7000).fit(arr_coordinates).labels_
    
    gpd_input['grp_loc'] = clusters

    return gpd_input

def group_area(gpd_input: gpd.GeoDataFrame):
    gpd_input = gpd_input.to_crs(crs='EPSG:8857')

    gpd_intersection = gpd.overlay(
        gpd_input, gpd_input,
        how='intersection',
        keep_geom_type=False,
    )['geometry'].area

    gpd_union = gpd.overlay(
        gpd_input, gpd_input,
        how='union',
        keep_geom_type=False,
    )['geometry'].area

    
    pass

def group_txt_loc(pl_input: pl.DataFrame,
                  link_lvl: str=None) -> pl.DataFrame:
    if link_lvl == 'state':
        pl_input = pl_input.with_columns(
            tmp = pl.concat_str(
                [pl.col('state_or_province', 'country')],
                separator='_'
            )
        )
    else:
        pl_input = pl_input.with_columns(
            tmp = pl.col('country')
        )

    list_countries = pl_input['tmp'].unique().to_list()
    list_numbers = list(range(len(list_countries)))

    dict_countries = dict(zip(list_countries, list_numbers))
    pl_input = pl_input.with_columns(
        pl.col('tmp').replace(dict_countries, default=-1).alias(f'grp_{link_lvl}')
    )

    return pl_input