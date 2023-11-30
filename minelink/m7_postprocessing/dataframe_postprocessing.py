import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from minelink.params import *
from minelink.m0_save_and_load.save_load_directory import check_dir
from minelink.m0_save_and_load.save_load_file import *
from minelink.m7_postprocessing.determine_location import *

def tmppostprocessing(df_linked, dict_info, dict_location, dict_sameas, dict_geom):
    dict_code_alias = load_file(PATH_TMP_DIR, 'code_alias', '.pkl')
    
    df_linked_group = df_linked[df_linked.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))
    df_linked_indiv = df_linked[df_linked.GroupID==-1].drop('GroupID', axis=1)

    return 0

def add_source_column(df_links):
    df_links['source'] = df_links['idx'].apply(lambda x: re.split('_', x)[0])

    return df_links

def get_item(idx, source, required_item):
    path_dict = os.path.join(PATH_TMP_DIR, source)
    dict_required = load_file(path_dict, required_item, '.pkl')

    return dict_required[idx]

def get_sameas(list_idx, list_source, dict_code_alias):
    compiled_sameas = {dict_code_alias[key]:[] for key in list_source}

    for i in range(len(list_idx)):
        source = dict_code_alias[list_source[i]]

        path_dict = os.path.join(PATH_TMP_DIR, list_source[i])
        dict_sameas = load_file(path_dict, 'same_as', '.pkl')
        dict_geom = load_file(path_dict, 'geometry', '.pkl')

        source_id = re.split('_', list_idx[i])[1]

        row_dict = {
            "id": source_id,
            "Attributes": dict_sameas[list_idx[i]],
            "geometry": dict_geom[list_idx[i]]['geometry'],
        }

        compiled_sameas[source].append(row_dict)

    return compiled_sameas

def group_dataframeitems(df_links, dict_code_alias):
    df_with_grouping = df_links[df_links.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))
    df_no_grouping = df_links[df_links.GroupID == -1].drop('GroupID', axis=1)
    df_no_grouping['idx'] = df_no_grouping['idx'].apply(lambda x: [x])
    df_no_grouping['source'] = df_no_grouping['source'].apply(lambda x: [x])

    df_grouped = pd.concat([df_no_grouping, df_with_grouping], axis=0, ignore_index=True)
    df_grouped['id'] = 'Site' + df_grouped.index.astype(str)

    # TODO: need sitename thing in preprocessing
    # df_grouped['name'] = df_grouped.apply(lambda x: get_item(x['idx'][0], x['source'][0], 'name'), axis=1)
    df_grouped['location_info'] = df_grouped.apply(lambda x: get_item(x['idx'][0], x['source'][0], 'location_info'), axis=1)
    df_grouped['same_as'] = df_grouped.apply(lambda x: get_sameas(x['idx'], x['source'], dict_code_alias), axis=1)

    # TODO: use the version with the name
    df_grouped = df_grouped[['id', 'location_info', 'same_as']]
    # df_grouped = df_grouped[['id', 'name', 'location_info', 'same_as']]

    return df_grouped

def postprocessing():
    df_links = load_file(PATH_TMP_DIR, 'df_links', '.pkl')
    dict_code_alias = load_file(PATH_TMP_DIR, 'code_alias', '.pkl')

    df_links = add_source_column(df_links)
    df_grouped = group_dataframeitems(df_links, dict_code_alias)

    check_dir(PATH_OUTPUT_DIR)
    dump_file(df_grouped, PATH_OUTPUT_DIR, 'linked_result', 'JSON')