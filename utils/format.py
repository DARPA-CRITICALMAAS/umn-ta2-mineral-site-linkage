import pandas as pd
import geopandas as gpd
import regex as re
import numpy as np
import functools
import operator

from utils.params import *
from utils.columns import *
from utils.document import *

def map_col_title(geo_dataframe, filename):
    """
    map_col_title function

    : input: geo_dataframe = 
    : input: filename = 

    : return: geo_dataframe = 
    """
    # dict_col_name, list_col_commodities = find_cols(filename)
    geo_dataframe = geo_dataframe.rename(columns={'Name_Dep': 'site_name', 'Name_Other':'other_name', 'depname':'site_name', 'names': 'other_name'})

    dict_col_name = {'ftr_name': 'site_name'}
    # list_col_commodities = ['commodity']
    col_commodities = set(['commodity', 'commod1', 'commod2', 'commod3'])
    actual = set(list(geo_dataframe.columns))

    list_col_commodities = list(col_commodities & actual)

    # Identify columns representing names of mines
    geo_dataframe = geo_dataframe.rename(columns=dict_col_name)

    # Combine columns representing commodities
    geo_dataframe.insert(loc=0, column='commodities', value='')

    for col in list_col_commodities:
        geo_dataframe[col] = geo_dataframe[col].replace(np.nan, '')
        geo_dataframe['commodities'] = geo_dataframe.apply(lambda x: (x.commodities + ', ' + x[col]) if x[col] != '' else x.commodities, axis=1)
        geo_dataframe = geo_dataframe.drop([col], axis=1)

    geo_dataframe['commodities'] = geo_dataframe['commodities'].apply(lambda x: [i.lstrip() for i in re.split(r'[^(\w & \s)]', str(x))])
                                                                                 
    return geo_dataframe

def match_crs(dataframe):
    """
    match_crs function

    : input: dataframe = 

    : return: dataframe = 
    """
    dataframe['geometry'] = dataframe['geometry'].to_crs(crs=CRS)

    return dataframe

def insert_basic(dataframe, filename):
    """
    insert_basic function

    : input: dataframe =
    : input: filename =

    : return: dataframe = 
    """
    dataframe.insert(loc=0, column='location_source', value=filename)
    dataframe.insert(loc=0, column='crs', value=CRS)

    dataframe['idx'] = dataframe['location_source']+ dataframe.index.astype('string')
    dataframe['index'] = dataframe.index.astype('string')

    return dataframe

def combine_name(geo_dataframe):
    """
    combine_name function

    : input: geo_dataframe = 
    
    : return: geo_dataframe = 
    """
    if 'other_name' in geo_dataframe.columns:
        geo_dataframe['other_name'] = geo_dataframe['other_name'].replace(np.nan, '')
        geo_dataframe['site_name'] = geo_dataframe.apply(lambda x: (x.site_name + '! ' + x.other_name) if x.other_name != '' else x.site_name, axis=1)
        geo_dataframe = geo_dataframe.drop(['other_name'], axis=1)

        # geo_dataframe['site_name'] = geo_dataframe['site_name'].apply(lambda x: list(filter(None, [i.lstrip() for i in re.split(r'[^(\w && \s && \d)]', str(x))])))
        geo_dataframe['site_name'] = geo_dataframe['site_name'].apply(lambda x: list(filter(None, [i.lstrip() for i in x.split('!')])))
    else:
        geo_dataframe['site_name'] = geo_dataframe['site_name'].apply(lambda x: [x])
    # geo_dataframe = geo_dataframe.explode('site_name')

    return geo_dataframe

def df_to_dict(geo_dataframe):
    """
    df_to_dict function

    : input: geo_dataframe = 
    
    : return: dict_df = 
    """
    dict_df = geo_dataframe.set_index('idx')
    dict_df = dict_df.to_dict('index')

    return dict_df

def merge_dict(dict1, dict2):
    """
    merge_dict function

    : input: dict1 = 
    : input: dict2 = 

    : return: merged_dict
    """
    dict1.update(dict2)
    merged_dict = dict1

    return merged_dict

def separate_dataframe(geo_dataframe, filename):
    """
    separate_dataframe function

    : input: geo_dataframe = 

    : return: dict_location = 
    : return: gpd_site = 
    : return: dict_sameas = 
    : return: dict_geo = 
    : return: df_doc
    """
    # df_matched = match_col_name(geo_dataframe)
    # TODO: Maybe match crs and insert basic should go in here to be applied only to df_matched

    gpd_sameas = geo_dataframe.drop(['location_source', 'crs', 'geometry', 'index'], axis = 1)
    dict_sameas = df_to_dict(gpd_sameas)

    geo_dataframe = map_col_title(geo_dataframe, filename)

    column_location = set(['idx', 'geometry', 'country', 'state', 'crs', 'location_source', 'index'])
    column_site = set(['idx', 'site_name', 'other_name', 'geometry', 'location_source', 'commodities']) # TODO: Maybe geometry is not needed in this 'country', 'state', 'county', 
    # column_doc = set(['idx', 'site_name', 'other_name', 'geometry', 'country', 'state', 'county'])
    column_doc_loc = set(['county', 'state', 'country'])
    actual = set(geo_dataframe.columns)

    # Return a list of columns relevant to location (used for document formation as of now)
    col_available_loc = list(column_doc_loc & actual)

    # Creating dictionary of locations
    gpd_location = geo_dataframe[list(column_location & actual)]
    gpd_location = gpd_location.rename(columns={'geometry': 'location', 
                                                'state': 'state_or_province', 
                                                'index': 'location_source_record_id'})
    dict_location = df_to_dict(gpd_location)

    # Creating geopandas dataframe of site information
    gpd_site = geo_dataframe[list(column_site & actual)]
    gpd_site = combine_name(gpd_site)
    gpd_site = gpd_site.reset_index(drop=True)

    # Creating dictionary of geometry
    gpd_geo = geo_dataframe[['idx', 'geometry']]
    dict_geo = df_to_dict(gpd_geo)

    # Creating dictionary of sameas
    # TODO: Maybe there needs to be a part before the column names that replicates the dataframe with matching column names

    # Creating dataframe with combined document embedding and location embedding
    # df_doc = convert_to_document(gpd_site, col_available_loc)
    # df_doc = geo_dataframe[list(column_doc & actual)]

    return dict_location, gpd_site, dict_sameas, dict_geo, col_available_loc    #, df_doc

def create_sameas(list_idx, list_source, dict_sameas, dict_geo):
    compiled_sameas = {key:[] for key in list_source}

    for i in list_idx:
        source = str(re.split('\d', i)[0])
        source_id = int(re.findall('\d+', i)[0])
        geom_data = dict_geo[i]['geometry']

        row_dict = {
            "id": source_id,
            "Attributes": dict_sameas[i],
            "geometry": geom_data,
        }

        compiled_sameas[source].append(row_dict)

    return compiled_sameas

def convert_to_output_form(df_site, dict_location, dict_sameas, dict_geo):

    df_site_w = df_site[df_site.GroupID != -1].groupby(['GroupID']).agg(lambda x: list(x))

    df_site_i = df_site[df_site.GroupID == -1].drop('GroupID', axis=1)
    df_site_i['site_name'] = df_site_i['site_name'].apply(lambda x: [x])
    df_site_i['idx'] = df_site_i['idx'].apply(lambda x: [x])
    df_site_i['location_source'] = df_site_i['location_source'].apply(lambda x: [x])

    df_site = pd.concat([df_site_w, df_site_i], ignore_index=True, axis=0)

    df_site['id'] = 'Site' + df_site.index.astype(str)
    df_site = df_site.set_index('id')

    df_site['name'] = df_site['site_name'].apply(lambda x: x[0][0])

    df_site = df_site.drop('site_name', axis=1)
    df_site['location_info'] = df_site['idx'].apply(lambda x: dict_location[x[0]])

    df_site['same_as'] = df_site.apply(lambda x: create_sameas(x['idx'], x['location_source'], dict_sameas, dict_geo), axis=1)
    df_site = df_site.drop(['location_source', 'idx'], axis=1).reset_index()
    df_site = df_site[['id', 'name', 'location_info', 'same_as']]

    return df_site

def dataframe_orderby(dataframe, col):
    dataframe = dataframe.sort_values(by=[col])
    dataframe = dataframe.reset_index(drop=True)

    list_group = dataframe['GroupID'].tolist()

    return dataframe, list_group
    
def format_dataframe(dataframe, filename):
    dataframe.columns = dataframe.columns.str.lower()
    dataframe = match_crs(dataframe)
    dataframe = insert_basic(dataframe, filename)
    # dict_location, gpd_site, dict_sameas, dict_geo, col_available_loc, df_doc = separate_dataframe(dataframe, filename)
    dict_location, gpd_site, dict_sameas, dict_geo, col_available_loc = separate_dataframe(dataframe, filename)

    return dict_location, gpd_site, dict_sameas, dict_geo, col_available_loc    #, df_doc

def select_location(row_dataframe, column_title):
    new_row = []
    
    for i in column_title:
        new_row.append(row_dataframe[i][0])

    return new_row

def merge_names(row_dataframe, column_title):
    new_row = []

    for i in column_title:
        flattened = np.array(row_dataframe[i]).flatten().tolist()
        new_row.extend(flattened)
    
    return new_row

def clean_commodities(list_commodities):
    list_commodities = functools.reduce(operator.concat, list_commodities)
    list_commodities = list(set(list_commodities))
    list_commodities = list(filter(None, list_commodities))
    # list_commodities = np.array(list_commodities).flatten().flatten().tolist()
    # print(list_commodities)
    return list_commodities

def select_geometry(list_geometry):
    return list_geometry[0]

def create_intra_rep(dataframe):
    column_site = set(['idx', 'site_name', 'other_name', 'geometry', 'location_source', 'country', 'state', 'county', 'commodities'])
    column_loc = set(['country', 'state', 'county'])
    column_name = set(['site_name', 'other_name'])
    actual = set(dataframe.columns)

    dataframe = dataframe[list(column_site & actual)]   # type is dataframe
    column_loc_available = list(column_loc & actual)
    column_name_available = list(column_name & actual)
    
    df_loc = dataframe.apply(lambda x: select_location(x, column_loc_available), axis=1)
    df_loc = df_loc.apply(pd.Series)
    df_loc.columns = column_loc_available

    df_name = dataframe.apply(lambda x: merge_names(x, column_name_available), axis=1)
    df_name.columns = 'site_names'

    df_comm = dataframe['commodities'].apply(lambda x: clean_commodities(x))
    df_geom = dataframe['geometry'].apply(lambda x: select_geometry(x))

    dataframe = dataframe.drop(column_loc_available, axis=1)
    dataframe = dataframe.drop(['commodities', 'geometry'], axis=1)
    dataframe = pd.concat([dataframe, df_loc, df_comm, df_geom], axis=1)

    df_doc = convert_to_document(dataframe, column_loc_available)

    # print(df_loc)
    # print(dataframe)

    # gpd_site = geo_dataframe[list(column_site & actual)]
    # gpd_site = combine_name(gpd_site)
    # gpd_site = gpd_site.reset_index(drop=True)

    # dataframe.to_csv('tmp.csv')

    return df_doc

# gpd_sample.to_csv('tmp.csv')