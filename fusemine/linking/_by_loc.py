import numpy as np
import polars as pl
import geopandas as gpd

from sklearn.cluster import HDBSCAN

def group_proximity(gpd_input: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    TODO: need to fill

    Arguments:
    gpd_input: 
    """
    # TODO: add the equidistance epsg
    gpd_input = gpd_input.to_crs(crs='EPSG:')
    arr_coordinates = np.array(list(zip(gpd_input.geometry.x, gpd_input.geometry.y)))
    # TODO: Add the cluster selection epsilon
    clusters = HDBSCAN(min_cluster_size=2,
                       cluster_selection_epsilon=0).fit(arr_coordinates).labels_
    
    gpd_input['grp_loc'] = clusters

    return gpd_input

def group_area():
    # TODO: add tthe area epsg
    gpd_input = gpd_input.to_crs(crs='EPSG:')

    gpd_intersection = gpd.overlayh(
        gpd_input, gpd_input
        how='intersection',
        keep_geom_type=False,
    )['geometry'].area

    gpd_union = gpd.overlayh(
        gpd_input, gpd_input
        how='union',
        keep_geom_type=False,
    )['geometry'].area

    
    pass