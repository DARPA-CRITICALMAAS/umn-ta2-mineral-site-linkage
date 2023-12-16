from minelink.params import *
from minelink.m0_load_input.save_ckpt import save_ckpt
from minelink.m2_intralinking.location_based import location_based_linking
from minelink.m2_intralinking.text_based import text_based_linking


def intralink(list_code, bool_location):
    for i in list_code:
        pl_loc_linked = location_based_linking(i, bool_location)

        if not bool_location:
            pl_doc = text_based_linking(i)

        save_ckpt(data=pl_doc,
                  list_path=[PATH_TMP_DIR, i],
                  file_name='pl_document')