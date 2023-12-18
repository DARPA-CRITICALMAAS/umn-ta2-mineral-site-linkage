import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt
from minelink.m2_intralinking.location_based import location_based_linking
from minelink.m2_intralinking.text_based import text_based_linking, global_embedding_reduction

def intralink(list_code, bool_location):
    for i in list_code:
        pl_loc_linked = location_based_linking(i, bool_location)
        pl_intra_linked = pl_loc_linked

        if not bool_location:
            pl_doc, pl_txt_linked = text_based_linking(i, pl_loc_linked)
            pl_intra_linked = pl_txt_linked

            save_ckpt(data=pl_doc,
                      list_path=[PATH_TMP_DIR, i],
                      file_name='pl_document')

        # pl_intra_linked = pl_intra_linked.group_by(
        #     'GroupID'
        # ).agg(
        #     [pl.col('idx')]
        # ).with_columns(
        #     group_idx = pl.lit(i+'_group_')+pl.col('GroupID').cast(pl.Utf8)
        # ).drop('GroupID')

        save_ckpt(data=pl_intra_linked,
                  list_path=[PATH_TMP_DIR, i],
                  file_name='pl_intra_linked')
        
def just_dimension(alias_code):
    pl_tolink = load_file([PATH_TMP_DIR, alias_code],
                          'pl_document',
                          '.pkl')

    list_reduced = global_embedding_reduction(pl_tolink)

    pl_reduced = pl_tolink.with_columns(
        reduced_1 = pl.Series(list_reduced)
    )

    save_ckpt(data=pl_reduced,
              list_path=[PATH_TMP_DIR, alias_code],
                  file_name='pl_reduced')
