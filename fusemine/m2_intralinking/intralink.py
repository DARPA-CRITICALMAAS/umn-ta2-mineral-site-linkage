import polars as pl

from fusemine.params import *
from fusemine.m0_load_input.load_data import load_file
from fusemine.m0_load_input.save_ckpt import save_ckpt
from fusemine.m2_intralinking.location_based import location_based_linking
from fusemine.m2_intralinking.text_based import text_based_linking

def intralink(list_code, bool_location):
    for i in list_code:
        pl_loc_linked = location_based_linking(i, bool_location)
        pl_intra_linked = pl_loc_linked.unique(
            subset=['idx'],
            maintain_order=True,
            keep="first"
        )

        if not bool_location:
            dict_known = load_file([PATH_SRC_DIR], 'dict_known', '.pkl')
                
            pl_intra_linked = text_based_linking(pl_loc_linked, i, dict_known)

        save_ckpt(data=pl_intra_linked,
                  list_path=[PATH_TMP_DIR, i],
                  file_name='pl_intra_linked')