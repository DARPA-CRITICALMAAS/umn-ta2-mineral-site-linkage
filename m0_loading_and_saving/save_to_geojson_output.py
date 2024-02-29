import os
import pickle
import geopandas as gpd
import shapely.wkt

from json import dump

def save_mineralsite_output_geojson(pl_processed_mineralsite, path_directory:str, file_name:str):
    """
    Saves the data in the mineralsite schema format as a geojson file that can be plotted on GIS software

    : param: df_mineralsite = 
    : param: file_name = 
    : param: list_path = 
    """
    if not os.path.exists(path_directory):
        os.makedirs(path_directory)

    df_processed_mineralsite = pl_processed_mineralsite.to_pandas()
    gs_mineralsite = gpd.GeoSeries.from_wkt(df_processed_mineralsite['location'])
    gdb_mineralsite = gpd.GeoDataFrame(df_processed_mineralsite, geometry=gs_mineralsite, crs='WGS84')

    gdb_mineralsite.to_file(os.path.join(path_directory, file_name+'.geojson'), driver='GeoJSON')