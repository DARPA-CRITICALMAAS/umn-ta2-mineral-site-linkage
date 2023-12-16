import pandas as pd
import polars as pl
import geopandas as gpd

from minelink.params import *
from minelink.m0_save_and_load.load_data import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt

import numpy as np

def create_text_representation_pl(source_alias_code):
    return 0

def create_intra_representation_pl(source_alias_code):
    pl_intra_linked = load_file(PATH_TMP_DIR, 'pl_intra_linked', '.pkl', additional=source_alias_code)
    gpd_geom = load_file(PATH_TMP_DIR, 'gpd_intra_linked', '.pkl', additional=source_alias_code)

    pl_intra_text = pl_intra_linked
    gpd_intra_geom = gpd_geom

    return pl_intra_text, gpd_intra_geom

    # df_intra_linked = pd.concat([df_intra_linked, df_geom], axis=1)
    # df_intra_group = df_intra_linked.groupby(by='GroupID').agg(lambda x: [x])

    # df_intra_geom = create_geom_representation(df_intra_group.geometry)
    # df_intra_text = create_text_representation(df_intra_group.embedding)

    # df_intra_rep = pd.concat([df_intra_geom, df_intra_text])    # Need to get index from somewhere
    
    # return df_intra_rep

def create_text_representation(df_text):
    df_intra_text = df_text.apply(lambda x: np.average(np.array(x), axis=1))

    return df_intra_text

def create_geom_representation(df_geom):
    df_intra_geom = df_geom.centroid

    return df_intra_geom

def create_intra_representation(source_alias_code):
    #TODO: open df_tolink file with GroupID
    # TODO: open geom file with GroupID

    df_intra_linked = load_file(PATH_TMP_DIR, 'df_intra_linked', '.pkl', additional=source_alias_code)
    df_geom = load_file(PATH_TMP_DIR, 'df_geom', '.pkl', additional=source_alias_code)

    df_intra_linked = pd.concat([df_intra_linked, df_geom], axis=1)
    df_intra_group = df_intra_linked.groupby(by='GroupID').agg(lambda x: [x])

    df_intra_geom = create_geom_representation(df_intra_group.geometry)
    df_intra_text = create_text_representation(df_intra_group.embedding)

    save_ckpt(df_intra_geom, PATH_TMP_DIR, 'df_geom_rep', additional=source_alias_code)
    save_ckpt(df_intra_text, PATH_TMP_DIR, 'df_text_rep', additional=source_alias_code)

    # df_intra_rep = pd.concat([df_intra_geom, df_intra_text])    # Need to get index from somewhere
    
    # return df_intra_rep