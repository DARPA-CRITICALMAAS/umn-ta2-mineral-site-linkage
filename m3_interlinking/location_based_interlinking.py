import configparser

import polars as pl
import pandas as pd
import geopandas as gpd
from shapely import wkt

config = configparser.ConfigParser()
config.read('../params.ini')
interlink_params = config['interlink.params']

def create_convex_hull_with_buffer(pl_intralinked_mineralsite, token:str):
    """
    Creates convex hull based on geolocation of intralinked points and creates a 5km buffer around the shape

    : param: pl_intralinked_mineralsite = polars dataframe of intralinked mineralsites
    : return: gpd_mineralsite = geopandas dataframe of the mineralsite record with polygon geometry created by convex hull
    """
    crs_value = pl_intralinked_mineralsite.item(0, 'crs')

    len_total_records = pl_intralinked_mineralsite.shape[0]
    len_indiv_records = pl_intralinked_mineralsite.filter(pl.col('GroupID') == -1).shape[0]
    list_individual_groupID = list(range(len_total_records, len_total_records+len_indiv_records))

    pl_individual_mineralsite = pl_intralinked_mineralsite.filter(
        pl.col('GroupID') == -1
    ).drop('GroupID').with_columns(
        GroupID = pl.Series(list_individual_groupID)
    )
    pl_intralinked_mineralsite = pl_intralinked_mineralsite.filter(
        pl.col('GroupID') != -1
    )

    pl_intralinked_mineralsite = pl.concat(
        [pl_intralinked_mineralsite, pl_individual_mineralsite],
        how='vertical'
    ).with_columns(
        GroupAlias = pl.lit(token) + pl.col('GroupID').cast(pl.Utf8)
    ).drop('GroupID')
    del(pl_individual_mineralsite)

    df_intralinked_mineralsite = pl_intralinked_mineralsite.to_pandas()
    df_intralinked_mineralsite['location'] = df_intralinked_mineralsite['location'].apply(wkt.loads)
    
    gpd_mineralsite = gpd.GeoDataFrame(
        df_intralinked_mineralsite, 
        geometry='location', 
        crs=crs_value
    ).to_crs(
        crs='EPSG:3857'
    )

    gpd_intralink_polygon = gpd_mineralsite.dissolve(
        'GroupAlias'
    ).convex_hull.buffer(int(interlink_params['INTERLINK_BUFFER_unit_meter']))
    gpd_intralink_polygon.name = 'poly_geometry'
    gpd_intralink_polygon = gpd_intralink_polygon.to_frame().reset_index().sort_values(by=['GroupAlias'])
    gpd_intralink_polygon = gpd_intralink_polygon.set_geometry('poly_geometry')

    return pl_intralinked_mineralsite, gpd_intralink_polygon

def compare_convex_hull_sizes(gpd_mineralsite_poly1, gpd_mineralsite_poly2):
    """
    
    : param: gpd_mineralsite_poly1 = geopandas dataframe of the mineralsite record with polygon geometry
    : param: gpd_mineralsite_poly2 = geopandas dataframe of the mineralsite record with polygon geometry
    : return: pl_interlinked_mineralsite = polars dataframe
    """
    gpd_overlapped_mineralsite = gpd.overlay(
        gpd_mineralsite_poly1, gpd_mineralsite_poly2,
        how='intersection', keep_geom_type=False
    )
    gpd_overlapped_mineralsite['overlapped_area'] = gpd_overlapped_mineralsite.area
    gpd_overlapped_mineralsite = gpd_overlapped_mineralsite.drop('geometry', axis=1)

    try:
        # Converting geopandas geodataframe into a polars dataframe
        pd_overlapped_mineralsite = pd.DataFrame(gpd_overlapped_mineralsite)
        pl_overlapped_mineralsite = pl.from_pandas(pd_overlapped_mineralsite)

        # 1. Check whether the overlapping region is over the area threshold value
        AREA_THRESHOLD = float(interlink_params['INTERLINK_AREA_OVERLAP_unit_sqmeter'])
        pl_overlapped_mineralsite = pl_overlapped_mineralsite.filter(
            pl.col('overlapped_area') > AREA_THRESHOLD
        ).sort(
            'overlapped_area'
        )
        
        # 2. Leave the one with the highest overlapping region if there are multiple overlapping regions for the same record
        pl_overlapped_mineralsite = pl_overlapped_mineralsite.unique(
            subset = ['GroupAlias_1'],
            maintain_order=True,
            keep='first'
        ).unique(
            subset = ['GroupAlias_2'],
            maintain_order=True,
            keep='first'
        )

        dict_inter_overlap = dict(zip(pl_overlapped_mineralsite['GroupAlias_1'], pl_overlapped_mineralsite['GroupAlias_2']))

        return dict_inter_overlap

    except:
        # Case where there is no overlapping region possible between the two geopandas dataframe
        # Just concatenate the two input geopandas dataframe

        return {}

def location_based_linking(pl_intralinked_mineralsite1, pl_intralinked_mineralsite2):
    """
    
    : param: pl_intralinked_mineralsite1 = 
    : param: pl_intralinked_mineralsite2 = 
    : return: pl_interlinked_mineralstie = 
    """
    pl_mineralsite1, gpd_mineralsite_poly1 = create_convex_hull_with_buffer(pl_intralinked_mineralsite1, 'one')
    pl_mineralsite2, gpd_mineralsite_poly2 = create_convex_hull_with_buffer(pl_intralinked_mineralsite2, 'two')

    dict_inter_overlap = compare_convex_hull_sizes(gpd_mineralsite_poly1, gpd_mineralsite_poly2)
    pl_interlinked_mineralsite = pl.concat(
        [pl_mineralsite1, pl_mineralsite2],
        how='diagonal'
    ).with_columns(
        pl.col('GroupAlias').replace(dict_inter_overlap)
    ).group_by(
        'GroupAlias'
    ).agg(
        [pl.all()]
    ).drop('GroupAlias')

    list_new_GroupID = list(range(pl_interlinked_mineralsite.shape[0]))
    pl_interlinked_mineralsite = pl_interlinked_mineralsite.with_columns(
        GroupID = pl.Series(list_new_GroupID)
    ).explode(
        pl.exclude('GroupID')
    )
    
    del (gpd_mineralsite_poly1, gpd_mineralsite_poly2)

    return pl_interlinked_mineralsite