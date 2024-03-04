import configparser

import polars as pl
import pandas as pd
import geopandas as gpd
import wkt

config = configparser.ConfigParser()
config.read('../params.ini')
interlink_params = config['interlink.params']

def create_convex_hull_with_buffer(pl_intralinked_mineralsite):
    """
    Creates convex hull based on geolocation of intralinked points and creates a 5km buffer around the shape

    : param: pl_intralinked_mineralsite = 
    : return:
    """
    # Create group name for each intralinked group
    pl_intralinked_mineralsite = pl_intralinked_mineralsite.group_by(
        'GroupID'
    ).with_columns(
        intralink_group = pl.col('source_id') + pl.lit('_g_') + pl.col('GroupID')
    )

    df_intralinked_mineralsite = pl_intralinked_mineralsite.to_pandas()
    df_intralinked_mineralsite['location'] = df_intralinked_mineralsite['location'].apply(wkt.loads)
    
    gpd_mineralsite = gpd.GeoDataFrame(
        df_intralinked_mineralsite, 
        geometry='location', 
        crs='WGS84'
    ).to_crs(
        epsg=3857
    )
    
    gpd_mineralsite = gpd_mineralsite.dissolve(
        'intra_GroupID'
    ).convex_hull.buffer(interlink_params['INTERLINK_BUFFER'])

#     gpd_poly.name = 'rep_geometry'
#     gpd_poly = gpd_poly.to_crs('WGS84').to_frame().reset_index().sort_values(by=['GroupAlias'])
#     gpd_poly = gpd_poly.set_geometry('rep_geometry')

    return 0

def compare_convex_hull_sizes(pl_mineralsite_w_convex_hull):
    return 0

def location_based_linking(pl_intralinked_mineralsite):
    

    return 0