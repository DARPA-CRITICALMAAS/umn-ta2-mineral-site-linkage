import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt
from minelink.m2_intralinking.location_based import location_based_linking
from minelink.m2_intralinking.text_based import text_based_linking

def intralink(list_code, bool_location, names_column, commod_column):
    print('enter intralinking')

    for i in list_code:
        print("starting ", i)
        pl_loc_linked = location_based_linking(i, bool_location)
        pl_intra_linked = pl_loc_linked.unique(
            subset=['idx'],
            maintain_order=True,
            keep="first"
        )

        print(pl_intra_linked['idx'].to_list()) 

        print("location linked for ", i)
                

        pl_intra_linked = text_based_linking(pl_loc_linked, i, names_column, commod_column)

        #     save_ckpt(data=pl_doc,
        #               list_path=[PATH_TMP_DIR, i],
        #               file_name='pl_document')
            
        #     save_ckpt(data=pl_reduced_embedding,
        #               list_path=[PATH_TMP_DIR, i],
        #               file_name='pl_umap_set')

        save_ckpt(data=pl_intra_linked,
                  list_path=[PATH_TMP_DIR, i],
                  file_name='pl_intra_linked')
        
        print("done ", i)