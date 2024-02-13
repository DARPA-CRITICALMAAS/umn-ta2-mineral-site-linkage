import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt
from minelink.m2_intralinking.location_based import location_based_linking
from minelink.m2_intralinking.text_based import text_based_linking

def intralink(list_code, bool_location):
    print('enter intralinking')

    for i in list_code:
        print("starting ", i)
        pl_loc_linked = location_based_linking(i, bool_location)
        pl_intra_linked = pl_loc_linked

        print("location linked for ", i)

        if not bool_location:
            if i == 'ac':
                names_column = ['Ftr_Name', 'Other_Name']
                commod_column = ['Commodity']
            elif i == 'ab':
                names_column = ['Site']
                commod_column = ['Commodities_main', 'Commodities_other']
            elif i == 'ag' or i == 'ai':
                names_column = ['Site_Name', 'Other_Name']
                commod_column = ['Commodity']
            elif i == 'mss':
                names_column = ['name']
                commod_column = []
            else:
                names_column = ['site_name', 'names']
                commod_column = ['commod1', 'commod2', 'commod3']
                

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