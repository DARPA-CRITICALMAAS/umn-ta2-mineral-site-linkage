import pandas as pd
from minelink.m2_location_based_linking.link_with_loc import link_with_loc
from minelink.m3_text_based_linking.embedding_reduction import text_embedding_reduction
from minelink.m3_text_based_linking.create_documents import create_documents

def link_with_all_pl(df_tolink):
    return 0

def link_with_all(df_tolink, path_stored):
    df_loc_linked = link_with_loc(df_tolink)
    df_Loc_linked = df_loc_linked.drop(['geometry'])
    df_loc_linked = df_loc_linked.groupby(by='GroupID').agg(lambda x: [x])

    df_loc_linked['count'] = df_loc_linked['idx'].apply(lambda x: len(x))
    df_loc_linked_i = df_loc_linked[df_loc_linked['count'] == 1]
    df_loc_linked_i = df_loc_linked_i.drop(['count'])    # remaining should only be groupID and index
    df_loc_linked_i = df_loc_linked_i.explode(by='GroupID')

    df_loc_linked_g = df_loc_linked[df_loc_linked['count'] != 1]
    df_loc_linked_g = df_loc_linked_g.drop(['count'])
    df_loc_linked_g = df_loc_linked_g.explode(by='GroupID')

    # create document and do embedding reduction only on those that more than 1 count
    # TODO: unravel the ones with count greater than 1
    # TODO: drop document, embedding, and geometry column of evenetually all of them to concatenate
    df_documentized = create_documents(df_loc_linked_g, path_stored)
    df_reduced_document = text_embedding_reduction(df_documentized)


    df_reduced_document.drop(['document', 'embedding', 'reduced_embedding'])

    df_linked = pd.concat([df_loc_linked_i, df_reduced_document])
    
    return df_linked