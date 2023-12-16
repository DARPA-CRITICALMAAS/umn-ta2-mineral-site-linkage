from minelink.params import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt
from minelink.m2_location_based_linking import link_with_loc
from minelink.m3_text_based_linking import *
from minelink.m3_text_based_linking.link_with_all import *
from minelink.m4_intralinking.create_intra_representation import create_representation

def inter_link_pl(list_files, bool_location):
    for i in list_files:
        create_representation(i)


    # return df_links
        

def inter_link(list_files, bool_location):
    for i in list_files:
        create_representation(i)

    if bool_location:
        df_links = link_with_loc(list_files, epsilon=0.005)
    else:
        df_links = interlink_with_all(list_files)

    return df_links