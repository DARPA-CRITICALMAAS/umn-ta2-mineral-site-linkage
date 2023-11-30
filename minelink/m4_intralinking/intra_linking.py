from minelink.m2_location_based_linking.link_with_loc import link_with_loc
# from minelink.m3_TextBasedLinking.link_with_all import *

def intra_link(df_tolink):
    df_linked = link_with_loc(df_tolink)

    df_links = df_linked[['idx', 'GroupID']]

    return df_links