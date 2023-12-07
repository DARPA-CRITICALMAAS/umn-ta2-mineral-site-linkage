import time

from minelink.m0_save_and_load.save_load_file import dump_file
from minelink.m2_location_based_linking.link_with_loc import link_with_loc
from minelink.m3_text_based_linking.create_documents import *
# from minelink.m3_TextBasedLinking.link_with_all import *

def intra_link(df_tolink, path_store, bool_location):
    if bool_location:
        df_linked = link_with_loc(df_tolink, epsilon=0.005)
    else:
        df_tolink = create_documents(df_tolink) # output would only consists of index, geometry, and document
        df_linked = link_with_loc(df_tolink)

    df_links = df_linked[['idx', 'GroupID']]

    dump_file(df_links, path_store, 'df_links', 'PICKLE')

    return df_links