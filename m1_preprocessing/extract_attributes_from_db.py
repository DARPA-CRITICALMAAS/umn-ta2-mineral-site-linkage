import os
import pickle
import configparser

import regex as re
import polars as pl
import polars.selectors as cs
from sentence_transformers import SentenceTransformer, util

from m0_loading_and_saving.load_local_data import open_local_files

config = configparser.ConfigParser()
config.read('../params.ini')
path_params = config['directory.paths']
ATTRIBUTE_DEF_SIMILARITY_THRESHOLD = float(config['threshold.values']['ATTRIBUTE_SIMILARITY_THRESHOLD'])

model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def identify_id_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    Identifies the attribute (column) that is representing the unique ID of the mineral site record.
    If there is no unique ID, it will provide a number for the mineral site

    : param: pl_mineralsite = polars dataframe of all the mineral site records
    : param: dict_attributes = dictionary that gives label and its definition
    : param: list_known_mappings = dictionary consisting of previous column mapping to unique id
    : return: pl_mineralsite: polars dataframe with modified attribute labels
    """

    # TODO: The site name is considered to be unique for USMIN. Possibly make it purely rely on attribute identification?
    # Gives a dataframe that only has unique items
    pl_unique_column = pl_mineralsite.select(
        pl.all().unique().count() == pl_mineralsite.shape[0]
    )

    list_potential_id = [col.name for col in pl_unique_column if col.all()]

    if len(list_potential_id) == 0:     # Case where there is no column with all unique items
        return False
    elif len(list_potential_id) == 1:   # Caase where there is one column with all unique items
        return list_potential_id[0]
    
    # Case where there are multiple columns with all unique items
    for c in list_potential_id:
        splitted_col_name = re.split('[^A-Za-z]', c.lower())

        if 'id' in splitted_col_name:   # Select the attribute with the word 'id'
            return c
        
    dict_uniqueid_attributes = {
        col_name:' unique_id'
    }
    dict_known_mappings.update(dict_uniqueid_attributes)

    return dict_known_mappings

def identify_name_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    Identifies the attribute that is representing the name and other name (i.e., name alias) of the mineral site
    If there are no name, it will be named 'Unknown'

    : param: pl_mineralsite
    : param: dict_attributes = dictionary that gives label and its definition
    : param: list_known_mappings = 
    : return: pl_mineralsite: polars dataframe with modified attribute labels
    """
    name_definition = "Current name of the site"
    name_embedding = model.encode(name_definition)

    # Selects columns that has 'string' type attribute
    pl_string_data = pl_mineralsite.select(
        ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    )
    set_string_based_attributes = set(list(pl_string_data.columns))

    # Check if any of the attribute labels in pl_mineralsite matches ones in known mappings
    list_attributes_known = list(set(list_known_mappings) & set_string_based_attributes)


    # pl_name = pl_data.select(
    #     pl.col(remaining_columns)
    # ).select(
    #     ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    # )

    # col_match = compare_description(dict_data, dict_target, potential_name, ['name'])
    
    # # Will be deleted
    # potential = set(remaining_columns) & set(['Ftr_Name', 'Site_Name', 'site_name', 'Site'])
    # potential = set(col_match) | potential
    
    # return list(potential)

    # name_target = list(dict_target.keys())
    # descrip_target = list(np.array(list(dict_target.values())).flatten())
    # emb_target = model.encode(descrip_target, convert_to_tensor=True)

    # name_against = list(dict_against.keys())
    # descrip_against = list(np.array(list(dict_against.values())).flatten())
    # emb_against = model.encode(descrip_against, convert_to_tensor=True)

    # cosine_scores = util.cos_sim(emb_target, emb_against)
    # cosine_scores = np.array(cosine_scores.tolist())

    # idx = list(dict.fromkeys(np.where(cosine_scores > 0.45)[1]))

    # col_match = np.array(name_against)[idx]

    dict_name_attributes = {
        col_name:'name'
    }
    dict_known_mappings.update(dict_name_attributes)

    return dict_known_mappings

def identify_geolocation_attribute(pl_mineralsite, list_known_mappings:list):
    """
    Identifies the attribute that is representing the latitude and longitude of the mineral site
    If there is no attributes representing such, it will return a None item

    : param: pl_mineralsite: polars dataframe of all the records from the mineral site data
    : param: dict_known_mappings (default=None) = 
    : return: pl_mineralsite: polars dataframe with modified attribute labels
    """

    latitude_definition = "Latitude in decimal degree"
    latitude_embedding = model.encode(latitude_definition)

    longitude_definition = "Longitude in decimal degree"
    longitude_embedding = model.encode(longitude_definition)

    crs_definition = "Coordinate reference system"
    crs_embedding = model.encode(crs_definition)

    dict_geolocation_attributes = {
        col_name: 'latitude',
        col_name: 'longitude',
    }

    # Update the stored dictionary
    dict_known_mappings.update(dict_geolocation_attributes)

    return pl_mineralsite

def identify_textlocation_attribute(pl_mineralsite):
    """
    Identifies the attribute that is representing the country and/or state of the mineral site

    : param: pl_mineralsite: polars dataframe of all the records from the mineral site data
    : param: dict_known_mappings (default=None) = 
    : return: pl_mineralsite: polars dataframe with modified attribute labels
    """
    dict_textlocation_attributes = {}

    for i in list(pl_mineralsite.columns):
        if i.lower() == 'country':
            dict_textlocation_attributes[i] = 'country'
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_textlocation_attributes[i] = 'state_or_province'

    pl_mapped_mineralsite = pl_mineralsite.rename(
        dict_textlocation_attributes
    )

    return pl_mapped_mineralsite

# TODO: Update to include these three
def identify_commodity_attribute(pl_mineralsite, list_known_mappings:list):
    """
    Identifies attribute(s) representing the commodity available at the mineral site

    : param: pl_mineralsite:
    : param: dict_known_mappings (default=None) = 
    : return: 
    """
    commodity_definition = "Commodities available at the mineral site"
    commodity_embedding = model.encode(commodity_definition)

    dict_commodity_attributes = {
        col_name:'commodity'
    }
    dict_known_mappings.update(dict_commodity_attributes)

    return dict_known_mappings


def identify_operationtype_attribute(pl_mineralsite, list_known_mappings:list):
    """
    
    """
    operationtype_definition = "Current operation status of the mineral site."
    operationtype_embedding = model.encode(operationtype_definition)

    dict_operationtype_attributes = {
        col_name:'operation_type'
    }
    dict_known_mappings.update(dict_operationtype_attributes)

    return dict_known_mappings

def identify_recordyear_attribute(pl_mineralsite, list_known_mappings:list):
    """
    
    """
    recordyear_definition = "Year the mineral site record was taken"
    recordyear_embedding = model.encode(recordyear_definition)

    list_known_mappings['record_year'].append(col_name)

    return list_known_mappings

####

def map_attribute_labels(pl_mineralsite, dict_attributes):
    """
    
    : param: pl_mineralsite = polars dataframe of the mineralsite record
    : param: dict_attributes = dictionary that gives label and its definition
    : return: pl_processed_mineralsite = polars dataframe with the attribute label mapped to the ones we have defined
    """
    dict_known_attribute_maps = open_local_files(path_params['PATH_RESOURCE_DIR'], 'attribute_dictionary', '.pkl')

    # map attribute label representing unique id as 'record_id'
    list_known_recordid = dict_known_attribute_maps['record_id']
    updated_list_known_recordid, pl_mineralsite = identify_id_attribute(pl_mineralsite, dict_attributes, list_known_recordid)

    # map attribute label reprepresenting site name as 'name', 'other_names'
    list_known_name = dict_known_attribute_maps['names']
    updated_list_known_name, pl_mineralsite = identify_name_attribute(pl_mineralsite, dict_attributes, list_known_name)

    # # map attribute label representing longitude and latitude as 'location'
    # if 'location' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_geolocation_attribute(pl_mineralsite, list_known_mappings:list)

    # # identify available attribute labels representing textual location
    # if 'location' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_textlocation_attribute(pl_mineralsite, list_known_mappings:list)

    # # identify available attribute labels representing textual location
    # if 'operation_type' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_operationtype_attribute(pl_mineralsite, list_known_mappings:list)

    # if 'record_year' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_recordyear_attribute(pl_mineralsite, list_known_mappings:list)


    pl_processed_mineralsite = pl_mineralsite

    # Storing the updated known attribute dictionary to the resource file
    with open(os.path.join(path_params['PATH_RESOURCE_DIR'], 'attribute_dictionary.pkl'), 'wb') as handle:
        pickle.dump(dict_known_attribute_maps, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return pl_processed_mineralsite