import polars as pl
import pandas as pd
import geopandas as gpd

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt

def create_loc_rep(pl_linked_data, pl_geom):
    pl_linked_data = pl_linked_data.sort('idx')
    pl_geom = pl_geom.sort('idx').drop('idx')
    pl_linked_data = pl.concat([pl_linked_data, pl_geom], how='horizontal')

    pl_individual = pl_linked_data.filter(
        pl.col('GroupID') == -1
    )

    pl_group = pl_linked_data.filter(
        pl.col('GroupID') != -1
    )

    pl_group_loc_rep = pl_group.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    ).select(
        intra_GroupID = pl.col('GroupID'),
        lat_rep = pl.col('latitude').list.mean(),
        long_rep = pl.col('longitude').list.mean(),
    )

    return pl_individual

def create_text_rep(pl_linked_data):
    pl_text_rep = pl_linked_data
    
    return pl_text_rep

def create_intra_rep(list_code, bool_loc):
    for i in list_code:
        pl_intra_linked = load_file([PATH_TMP_DIR, i],
                                    'pl_intra_linked',
                                    '.pkl')
        gpd_geom = load_file([PATH_TMP_DIR, i],
                             'df_geometry',
                             '.pkl')
        
        pl_loc_rep = create_loc_rep(pl_intra_linked, gpd_geom)
        save_ckpt(data=pl_loc_rep,
                  list_path=[PATH_TMP_DIR, i],
                  file_name='rep_loc')

        # if not bool_loc:
        #     pl_text_rep = create_text_rep(pl_intra_linked)
        #     save_ckpt(data=pl_text_rep,
        #             list_path=[PATH_TMP_DIR, i],
        #             file_name='rep_text')