import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt
from minelink.m3_interlinking.location_based_methods import clustering_centroid, overlapping_region

def interlink(list_code, bool_location):
    # pl_geom_all = pl.DataFrame()
    # pl_text_all = pl.DataFrame()

    # for i in list_code:
    #     pl_geom_cur = load_file([PATH_TMP_DIR, i],
    #                             'rep_loc',
    #                             '.pkl')
    #     pl_geom_all = pl.concat(
    #         [pl_geom_all, pl_geom_cur],
    #         how='vertical',
    #     )

    #     if not bool_location:
    #         pl_text_cur = load_file([PATH_TMP_DIR, i],
    #                                 'rep_text',
    #                                 '.pkl')
            
    #         pl_text_all = pl.concat(
    #             [pl_text_all, pl_text_cur],
    #             how='vertical'
    #         )

    # inter_loc_linked = location_based_linking(pl_geom_all, bool_location)

    # pl_interlinked = inter_loc_linked

    # save_ckpt(data=pl_interlinked,
    #           list_path=[PATH_TMP_DIR],
    #           file_name='pl_linked')

    # if not bool_location:
    #     text_based_linking(pl_text_all)

    # return inter_loc_linked

    for i in list_code:
        pl_intra_linked = load_file([PATH_TMP_DIR, i],
                                    'pl_intra_linked',
                                    '.pkl')
        gpd_geom = load_file([PATH_TMP_DIR, i],
                             'df_geometry',
                             '.pkl')

def inter_location():
    return 0

def inter_text():
    return 0