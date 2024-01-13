import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt
from minelink.m3_interlinking.location_based_methods import clustering_centroid, overlapping_region

def interlink(list_code, bool_location):
    for i in list_code:
        pl_intra_linked = load_file([PATH_TMP_DIR, i],
                                    'pl_intra_linked',
                                    '.pkl')
        gpd_geom = load_file([PATH_TMP_DIR, i],
                             'df_geometry',
                             '.pkl')
        
        if not bool_location:
            print(bool_location)

def inter_location():
    return 0

def inter_text():
    return 0