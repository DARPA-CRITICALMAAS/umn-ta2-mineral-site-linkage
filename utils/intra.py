import numpy as np
import pandas as pd

from utils.params import *
from utils.load_save import *
from utils.format import *
from utils.grouping import *
from utils.embedding import *

def reindex_indiv(dataframe):
    df_grouped = dataframe[dataframe['GroupID'] != -1]
    df_indiv = pd.DataFrame(dataframe[dataframe['GroupID'] == -1])

    len_total = dataframe.shape[0]
    len_indiv = dataframe[dataframe['GroupID'] == -1]['idx'].shape[0]

    list_newgroup = list(range(len_total, len_total+len_indiv))
    df_indiv['GroupID'] = list_newgroup

    dataframe = pd.concat([df_grouped, df_indiv], axis=0)
    # dataframe = dataframe.sort_index()

    # dataframe_w = dataframe[dataframe.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))

    # dataframe_i = dataframe[dataframe.GroupID == -1].drop('GroupID', axis=1)
    # dataframe_i['site_name'] = dataframe_i['site_name'].apply(lambda x: [x])
    # dataframe_i['idx'] = dataframe_i['idx'].apply(lambda x: [x])
    # dataframe_i['location_source'] = dataframe_i['location_source'].apply(lambda x: [x])

    # dataframe = pd.concat([dataframe_w, dataframe_i], ignore_index=True, axis=0)

    return dataframe

def same_location(dataframe):
    """
    'same_location' function groups items in the row if and only if the dataframe has the EXACT SAME location

    : input: dataframe = 

    : return: dataframe = 
    """
    dataframe = dataframe.groupby(['geometry']).agg(lambda x: list(x))

    list_group = list(range(0, dataframe.shape[0]))
    dataframe['GroupID'] = list_group
    
    dataframe = reindex_indiv(dataframe)

    return dataframe

def text_group(dataframe):
    """
    'text_group' function attempts to create groups within those that are already clusted based on location
    
    : input: dataframe = 

    : return: dataframe = 
    """
    # dataframe['type'] = dataframe['reduct_text'].apply(lambda x: type(x))
    # print(dataframe['type'])

    dataframe = tsne(dataframe)
    
    # dataframe = dataframe.to_crs(epsg='3857')
    df_combined = pd.concat([dataframe['geometry'].x, dataframe['geometry'].y], axis=1)
    df_combined.columns = ['X', 'Y']
    # coords = df_combined[['X', 'Y']].to_numpy()

    # df_combined = dataframe['geometry'].apply(lambda l: spatial_embedding(l.x, l.y))
    # df_combined = df_combined.apply(pd.Series)
    # df_combined.columns = ['X1', 'X2', 'Y1', 'Y2', 'Z1', 'Z2', 'K1', 'K2']

    tsne_text = dataframe['reduct_text'].apply(pd.Series)
    tsne_text.columns = ['A', 'B', 'C']

    df_combined = pd.concat([df_combined, tsne_text], axis=1)
    coords = df_combined.to_numpy()

    labels = db(coords)
    dataframe = pd.concat([dataframe, labels], axis=1)
    
    # print(coords.shape)

    # print(coords.shape)
    # dataframe = location_group(dataframe)
    # dataframe = dataframe.groupby(['GroupID']).agg(lambda x: list(x)).reset_index()
    # length = dataframe.shape[0]

    # # dataframe['reduct_text'] = dataframe['text'].apply(lambda x: princip_comp_analysis(x))

    # dataframe['newGroup'] = dataframe.apply(lambda x: hdb(length, x['GroupID'], x['reduct_text']) if len(x['idx']) > 1 else [x['GroupID'] + 1], axis=1)
    # dataframe = dataframe.drop(['GroupID'], axis=1)
    # dataframe = dataframe.rename(columns={'newGroup': 'GroupID'})
    # col_df = list(dataframe.columns)

    # dataframe = dataframe.explode(col_df)

    return dataframe

def common_parts(name):
    list_common = ['mine', 'shaft', 'no', 'co', 'deposit', 'district', 'and', 'pit', 'quarry', 'quarrie', 'prospect']

    if name.lower().rstrip('s') in list_common:
        return False
    return True

def close_words(name1, name2):
    count_match = len(set(name1) & set(name2))
    count_min = min(len(name1), len(name1))
    # print(name1, name2, count_min, count_match)
    if count_min != 0 and count_match/count_min > 0.5:
        return True

    joined_name1 = " ".join(name1).lower()
    joined_name2 = " ".join(name2).lower()
    if distance(joined_name1, joined_name2) <= 1:
        return True
    return False

def cleaned_name(group_num, len_total, list_names, dict_map):
    list_cleaned_sets = []
    # print(list_names)

    for i in list_names:
        set_cleaned = re.split(r'[^(A-Za-z)]', i)
        set_cleaned = list(filter(None, set_cleaned))
        set_cleaned = list(filter(common_parts, set_cleaned))
        list_cleaned_sets.append(set_cleaned)

    len_row = np.arange(1, len(list_cleaned_sets))
    len_full = np.arange(0, len(list_cleaned_sets))

    set_pairs = {key:[] for key in len_full}
    for row in len_row:
        for col in np.arange(0, row):
            # print(col, row, list_cleaned_sets[row], list_cleaned_sets[col], close_words(list_cleaned_sets[row], list_cleaned_sets[col]))
            if close_words(list_cleaned_sets[row], list_cleaned_sets[col]) and col in set_pairs and row in set_pairs:
                set_pairs[col].append(row)
                del set_pairs[row]

    values = list(set_pairs.values())
    keys = list(set_pairs.keys())

    org_groups = list(set(dict_map.values()))
    org_index = list(set(dict_map.keys()))

    reversed_dict = {}
    for key, value in dict_map.items():
        reversed_dict.setdefault(value, [])
        reversed_dict[value].append(key)
    
    new_group_num = group_num + (np.arange(0, len(org_groups)) * len_total)
    grouping = dict(zip(org_groups, new_group_num))

    for i in keys:
        if set_pairs[i]:
            for j in set_pairs[i]:
                grouping[dict_map[j]] = grouping[dict_map[i]]

    return list(grouping.values())


def gen_new_group(group_num, len_total, list_names):
    flat_list_name = list(np.concatenate(list_names).flat)
    org_keys = list(np.arange(0, len(flat_list_name)))

    org_values = []
    for i in range(0, len(list_names)):
        count = len(list_names[i])
        org_values.extend(np.repeat(i, count))

    dict_map = dict(zip(org_keys, org_values))

    return cleaned_name(group_num, len_total, flat_list_name, dict_map)

def intra_group(filename1, filename2, output_filename):
    df_test1 = pickle_load('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl', filename1)
    dict_loc, gpd_site, dict_sameas, dict_geo, col_available_loc = format_dataframe(df_test1, filename1)

    df_test2 = pickle_load('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl', filename2)
    dict_loc2, gpd_site2, dict_sameas2, dict_geo2, col_available_loc2 = format_dataframe(df_test2, filename2)

    gpd_site = pd.concat([gpd_site, gpd_site2], ignore_index=True)
    dict_loc.update(dict_loc2)
    dict_sameas.update(dict_sameas2)
    dict_geo.update(dict_geo2)

    gpd_site = location_group(gpd_site)
    gpd_grouped = gpd_site.groupby(['GroupID']).agg(lambda x: list(x))

    gpd_grouped['loc_group'] = gpd_grouped.index
    gpd_grouped.reset_index(drop=True, inplace=True)

    gpd_individual = gpd_grouped[gpd_grouped['loc_group'] == -1]
    gpd_individual = gpd_individual.drop('loc_group', axis=1)
    gpd_individual = gpd_individual.explode(list(gpd_individual.columns))
    gpd_individual.insert(loc=0, column='GroupID', value=-1)
    
    gpd_with_group = gpd_grouped[gpd_grouped['loc_group'] != -1]
    gpd_with_group['NewGroupID'] = gpd_with_group.apply(lambda x: gen_new_group(x['loc_group'], gpd_grouped.shape[0], x['site_name']), axis=1)

    gpd_with_group = gpd_with_group.drop('loc_group', axis=1)
    gpd_with_group = gpd_with_group.explode(list(gpd_with_group.columns))
    gpd_with_group = gpd_with_group.rename(columns={'NewGroupID': 'GroupID'})

    df_site = pd.concat([gpd_individual, gpd_with_group], axis=0)
    df_site.reset_index(drop=True, inplace=True)

    # df_site = location_group(gpd_site)

    df_final = convert_to_output_form(df_site, dict_loc, dict_sameas, dict_geo)
    json_dump(df_final, './outputs', output_filename)

    return reindex_indiv(df_site), dict_loc, dict_sameas, dict_geo

def intra_group_s(filename1, output_filename):
    df_test1 = pickle_load('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl', filename1)
    dict_loc, gpd_site, dict_sameas, dict_geo, col_available_loc = format_dataframe(df_test1, filename1)

    gpd_site = location_group(gpd_site)
    
    gpd_grouped = gpd_site.groupby(['GroupID']).agg(lambda x: list(x))

    gpd_grouped['loc_group'] = gpd_grouped.index
    gpd_grouped.reset_index(drop=True, inplace=True)

    gpd_individual = gpd_grouped[gpd_grouped['loc_group'] == -1]
    gpd_individual = gpd_individual.drop('loc_group', axis=1)
    gpd_individual = gpd_individual.explode(list(gpd_individual.columns))
    gpd_individual.insert(loc=0, column='GroupID', value=-1)
    
    gpd_with_group = gpd_grouped[gpd_grouped['loc_group'] != -1]
    gpd_with_group['NewGroupID'] = gpd_with_group.apply(lambda x: gen_new_group(x['loc_group'], gpd_grouped.shape[0], x['site_name']), axis=1)

    gpd_with_group = gpd_with_group.drop('loc_group', axis=1)
    gpd_with_group = gpd_with_group.explode(list(gpd_with_group.columns))
    gpd_with_group = gpd_with_group.rename(columns={'NewGroupID': 'GroupID'})

    df_site = pd.concat([gpd_individual, gpd_with_group], axis=0)
    df_site.reset_index(drop=True, inplace=True)

    # df_site = location_group(gpd_site)  # If only location
    df_final = convert_to_output_form(df_site, dict_loc, dict_sameas, dict_geo)
    json_dump(df_final, './outputs', output_filename)

    # return df_final, dict_loc, dict_sameas, dict_geo
    return reindex_indiv(df_site), dict_loc, dict_sameas, dict_geo


def intra_rep(dataframe, filename):
    # TODO: use group by clause to group, 'list'
    # TODO: combine name and combine commodities
    # TODO: select the most 'called' location or use the first one
    # TODO: create new sentence, sentence embedding, location embedding, combination for each of them
    # TODO: save them to pickle file

    dataframe = dataframe.groupby(['GroupID']).agg(lambda x: list(x))
    dataframe['site_name'] = dataframe['site_name'].apply(lambda x: list(np.concatenate(x).flat))
    pickle_dump(dataframe, './'+filename, 'df_rep')

    # print(dataframe)

    # dataframe = dataframe.drop(['text', 'location', 'reduct_text'], axis=1)
    # dataframe = create_intra_rep(dataframe)

    # pickle_dump(dataframe, './'+filename, 'df_rep')

    return dataframe
    
def intra_out(filename):
    """
    'intra_out' function selects a representative from each grouping and makes one combined embedding that represents the intra-grouped data

    : input: dataframe:

    : return: df_rep: 
    """

    doc = 'df_doc'
    loc = 'dict_location'
    sameas = 'dict_sameas'
    geo = 'dict_geo'

    directory = os.path.join('.', filename)
    if not os.path.exists(directory):
        os.makedirs(directory)

    df_doc = pickle_load(directory, doc)
    dict_loc = pickle_load(directory, loc)
    dict_sameas = pickle_load(directory, sameas)
    dict_geo = pickle_load(directory, geo)

    df_site = text_group(df_doc)

    # intra_group(filename)

    df_final = convert_to_output_form(df_site, dict_loc, dict_sameas, dict_geo)
    json_dump(df_final, './outputs', filename)

    return reindex_indiv(df_site)