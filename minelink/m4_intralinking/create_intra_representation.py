from minelink.params import *
from minelink.m0_save_and_load.load_data import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt

def create_representation(source_alias_code):
    df_intra_linked = load_file(PATH_TMP_DIR, 'df_intra_linked', '.pkl', additional=source_alias_code)
    df_tolink = load_file(PATH_TMP_DIR, 'df_tolink', '.pkl', additional=source_alias_code)

    #TODO: append groupID to original df_link
    df_intra_rep = df_intra_linked

    save_ckpt(df_intra_rep, PATH_TMP_DIR, 'df_rep', additional=source_alias_code)
    return df_intra_rep