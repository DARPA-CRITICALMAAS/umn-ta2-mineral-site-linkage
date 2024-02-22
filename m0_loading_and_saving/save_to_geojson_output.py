import os
import pickle
import geopandas as gpd

from json import dump


def save_mineralsite_output_geojson(gdb_site_data, file_name:str, list_path=[]):
    """
    Saves the data in the mineralsite schema format as a geojson file that can be plotted on GIS software

    : params: gdb_site_data
    : params: file_name
    : params: list_path
    """
    path_dir = ''

    for i in list_path:
        path_dir = os.path.join(path_dir, i)
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

    gdb_site_data.to_file(os.path.join(path_dir, file_name+'.geojson'), driver='GeoJSON')