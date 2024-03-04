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
    : return: gpd_mineralsite = geopandas dataframe of the mineralsite record with 
    """
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

    print('rename the buffer to polygon_geometry')

    return gpd_mineralsite

def compare_convex_hull_sizes(gpd_mineralsite_poly1, gpd_mineralsite_poly2):
    """
    
    : param: gpd_mineralsite_poly1 = geopandas dataframe 
    : param: gpd_mineralsite_poly2 = geopandas dataframe
    : return: pl_interlinked_mineralsite = polars dataframe
    """
    gpd_mineralsite_poly1 = gpd_mineralsite_poly1.rename(columns={'GroupID', 'intragroup_id1'})
    gpd_mineralsite_poly2 = gpd_mineralsite_poly2.rename(columns={'GroupID', 'intragroup_id2'})

    gpd_overlapped_mineralsite = gpd.overlay(
        gpd_mineralsite_poly1, gpd_mineralsite_poly2,
        how='intersection', keep_geom_type=False
    )

    try:
        # Converting geopandas geodataframe into a polars dataframe
        pd_overlapped_mineralsite = pd.DataFrame(gpd_overlapped_mineralsite)
        pl_overlapped_mineralsite = pl.from_pandas(pd_overlapped_mineralsite)

        # 1. Check whether the overlapping region is over the area threshold value
        AREA_THRESHOLD = interlink_params['INTERLINK_AREA_OVERLAP']
        pl_overlapped_mineralsite = pl_overlapped_mineralsite.filter(
            pl.col('overlapped_area') > AREA_THRESHOLD
        ).sort(
            'overlapped_area'
        )
        
        # 2. Leave the one with the highest overlapping region if there are multiple overlapping regions for the same record
        pl_overlapped_mineralsite = pl_overlapped_mineralsite.unique(
            subset = ['intragroup_id1'],
            maintain_order=True,
            keep='first'
        ).unique(
            subset = ['intragroup_id2'],
            maintain_order=True,
            keep='first'
        )

        # 3. Concatenate the two mineralsite records based on interlinking results
        pd_nonoverlapped_mineralsite1 = pd.DataFrame(gpd_mineralsite_poly1)
        pd_nonoverlapped_mineralsite2 = pd.DataFrame(gpd_mineralsite_poly2)
        pd_nonoverlapped_mineralsite = pd.concat(
            [pd_nonoverlapped_mineralsite1, pd_nonoverlapped_mineralsite2],
            axis=0, ignore_index=True
        )
        pl_nonoverlapped_mineralsite = pl.from_pandas(pd_nonoverlapped_mineralsite)

        pl_interlinked_mineralsite = pl.concat(
            [pl_overlapped_mineralsite, pl_nonoverlapped_mineralsite],
            how='vertical'
        ).drop('area')

        del (pd_overlapped_mineralsite, pl_overlapped_mineralsite, 
             pd_nonoverlapped_mineralsite, pd_nonoverlapped_mineralsite1, pd_nonoverlapped_mineralsite2)

        return pl_interlinked_mineralsite

    except:
        # Case where there is no overlapping region possible between the two geopandas dataframe
        # Just concatenate the two input geopandas dataframe
        df_mineralsite1 = pd.DataFrame(gpd_mineralsite_poly1)
        df_mineralsite2 = pd.DataFrame(gpd_mineralsite_poly2)

        pd_interlinked_mineralsite = pd.concat(
            [df_mineralsite1, df_mineralsite2],
            axis=0, ignore_index=True
        )

        del (df_mineralsite1, df_mineralsite2)

        return pl.from_pandas(pd_interlinked_mineralsite)

def location_based_linking(list_intralinked_mineralsite):
    

    return 0