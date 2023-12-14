import time

from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt
from minelink.m2_location_based_linking.link_with_loc import link_with_loc
from minelink.m3_text_based_linking.link_with_all import link_with_all

def intra_link(df_tolink, path_stored, bool_location):
    if bool_location:
        df_linked = link_with_loc(df_tolink, epsilon=0.005)
    else:
        df_linked = link_with_all(df_tolink, path_stored)

    df_links = df_linked[['idx', 'GroupID']]

    save_ckpt(df_links, path_stored, 'df_intra_linked')

    return df_links