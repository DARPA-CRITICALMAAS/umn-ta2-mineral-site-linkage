import os
import regex as re
import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import open_ckpt_dir

def get_info(list_idx, split_keyword, dict_alias):
    source_id, record_id = re.split(split_keyword, list_idx[0])

    basic_info = load_file([PATH_TMP_DIR, source_id], 'basic_info', '.pkl')
    location_info = load_file([PATH_TMP_DIR, source_id], 'location_info', '.pkl')

    rep_basic = basic_info[list_idx[0]]
    rep_location = location_info[list_idx[0]]

    same_as = []
    for i in list_idx:
        source_id, record_id = re.split(split_keyword, i)
        same_as.append({'source_id': dict_alias[source_id], 'record_id': record_id})

    return rep_basic['name'], rep_basic['source_id'], rep_basic['record_id'], rep_location, same_as

def postprocessing(list_code, bool_interlink):
    if bool_interlink:
        linked_file_name = 'pl_linked'
        pl_linked = load_file([PATH_TMP_DIR], linked_file_name, '.pkl')
        split_keyword = '_group_'
    else:
        linked_file_name = 'pl_intra_linked'
        pl_linked = load_file([PATH_TMP_DIR, 'aa'], linked_file_name, '.pkl')
        split_keyword = '_'
    
    dict_alias = load_file([PATH_TMP_DIR], 'alias_code', '.pkl')

    pl_output = pl_linked.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    ).map_rows(
        lambda x: get_info(x[1], split_keyword, dict_alias)
    ).rename(
        {'column_0': 'name',
         'column_1': 'source_id',
         'column_2': 'record_id',
         'column_3': 'location_info',
         'column_4': 'same_as'}
    )

    df_output = pl_output.to_pandas()

    return df_output
