import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from minelink.params import *
from minelink.m0_save_and_load.save_load_directory import check_dir
from minelink.m0_save_and_load.save_load_file import *
from minelink.m7_postprocessing.determine_location import *

def add_source_column(df_links):
    df_links['source'] = df_links['idx'].apply(lambda x: re.split('_', x)[0])

    return df_links

def get_item(idx, source, required_item, bool_single):
    if isinstance(idx, list):
        list_compiled = []

        for i in range(len(idx)):
            path_dict = os.path.join(PATH_TMP_DIR, source[i])
            dict_sameas = load_file(path_dict, required_item, '.pkl')
            
            list_compiled.append(dict_sameas[idx[i]])

        return list_compiled

    path_dict = os.path.join(PATH_TMP_DIR, source)
    dict_required = load_file(path_dict, required_item, '.pkl')

    if bool_single:
        return dict_required[idx]['0']

    return dict_required[idx]

def get_sameas(list_idx, list_source, dict_code_alias):
    compiled_sameas = []

    for i in range(len(list_idx)):
        path_dict = os.path.join(PATH_TMP_DIR, list_source[i])
        dict_sameas = load_file(path_dict, 'same_as', '.pkl')
        
        compiled_sameas.append(dict_sameas[list_idx[i]])

    return compiled_sameas

def group_dataframe_items(df_links, dict_code_alias):
    df_with_grouping = df_links[df_links.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))
    df_no_grouping = df_links[df_links.GroupID == -1].drop('GroupID', axis=1)
    df_no_grouping['idx'] = df_no_grouping['idx'].apply(lambda x: [x])
    df_no_grouping['source'] = df_no_grouping['source'].apply(lambda x: [x])

    df_grouped = pd.concat([df_no_grouping, df_with_grouping], axis=0, ignore_index=True)
    df_grouped['id'] = 'Site' + df_grouped.index.astype(str)

    df_grouped['name'] = df_grouped.apply(lambda x: get_item(x['idx'][0], x['source'][0], 'site_name', True), axis=1)
    df_grouped['location_info'] = df_grouped.apply(lambda x: get_item(x['idx'][0], x['source'][0], 'location_info', False), axis=1)
    df_grouped['same_as'] = df_grouped.apply(lambda x: get_item(x['idx'], x['source'], 'same_as', False), axis=1)

    df_grouped = df_grouped[['id', 'name', 'location_info', 'same_as']]

    return df_grouped

def postprocessing():
    df_links = load_file(PATH_TMP_DIR, 'df_links', '.pkl')
    dict_code_alias = load_file(PATH_TMP_DIR, 'code_alias', '.pkl')

    df_links = add_source_column(df_links)
    df_grouped = group_dataframe_items(df_links, dict_code_alias)

    check_dir(PATH_OUTPUT_DIR)
    dump_file(df_grouped, PATH_OUTPUT_DIR, 'linked_result', 'JSON')