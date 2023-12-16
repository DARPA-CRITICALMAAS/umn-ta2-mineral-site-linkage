import pandas as pd
import polars as pl

from minelink.params import *
from minelink.m0_save_and_load.load_data import load_file
from minelink.m2_location_based_linking.link_with_loc import link_with_loc
from minelink.m3_text_based_linking.embedding_reduction import text_embedding_reduction
from minelink.m3_text_based_linking.text_clustering import *
from minelink.m3_text_based_linking.create_documents import create_documents

def intralink_with_all_pl(path_stored):
    pl_tolink = load_file(path_stored, 'pl_tolink')

    pl_loc_linked = link_with_loc(pl_tolink)


# TODO: I think df_tolink is not needed but just need path_stored and load the df_toLink file from the path_store
def intralink_with_all(df_tolink, path_stored):
    df_geometry = load_file(path_stored, 'df_geometry')
    df_tolink = load_file(path_stored, 'df_tolink')

    df_tolink = pd.concat([df_tolink, df_geometry], axis=1)

    df_loc_linked = link_with_loc(df_tolink)
    df_Loc_linked = df_loc_linked.drop(['geometry'])

    df_loc_linked_i = df_loc_linked[df_loc_linked['GroupID'] == -1]
    df_loc_linked_g = df_loc_linked[df_loc_linked['GroupID'] != -1]

    df_loc_linked_g = df_loc_linked_g.groupby(by='GroupID').agg(lambda x: [x])


    # create document and do embedding reduction only on those that more than 1 count
    # TODO: unravel the ones with count greater than 1
    # TODO: drop document, embedding, and geometry column of evenetually all of them to concatenate
    df_documentized = create_documents(df_loc_linked_g, path_stored)
    df_reduced_document = text_embedding_reduction(df_documentized)
    df_linked_g = determine_cluster(df_reduced_document)

    # TODO: Think embedding should not be dropped
    df_linked_g.drop(['document', 'embedding', 'reduced_embedding'])

    df_linked = pd.concat([df_loc_linked_i, df_linked_g], axis=0)
    
    return df_linked

def interlink_with_all(list_files):
    # TODO: apply pca and umap reduction on combined embedding
    # TODO: hmmmm matching hmmm
    """
    input dataframe should have index, document, embedding, list(geometry)
    input should be list of files
    """

    df_full = pd.DataFrame()

    for i in list_files:
        df_tmp = load_file(PATH_TMP_DIR, 'df_text_rep', additional=i)
        df_geom = load_file(PATH_TMP_DIR, 'df_geom_rep', additional=i)

        df_tmp = pd.concat([df_tmp, df_geom], axis=1)
        df_full = pd.concat([df_tmp, df_full], axis=0)

    # link with location, location linking value may need to be larger? need to be tested on
    df_reduced_document = text_embedding_reduction(df_full)
    df_linked = determine_cluster(df_reduced_document)

    return df_linked