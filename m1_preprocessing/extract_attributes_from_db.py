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

def identify_id_attribute(pl_mineralsite, known_mappings=None):
    """
    Identifies the attribute (column) that is representing the unique ID of the mineral site record.
    If there is no unique ID, it will provide a number for the mineral site

    : param: pl_mineralsite = 
    : param: known_mappings (default=None) = 
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

    return pl_mineralsite

def identify_name_attribute(pl_mineralsite, known_mappings=None):
    """
    Identifies the attribute that is representing the name and other name (i.e., name alias) of the mineral site
    If there are no name, it will be named 'Unknown'

    : param: pl_mineralsite
    : param: known_mappings (default=None) = 
    : return: pl_mineralsite: polars dataframe with modified attribute labels
    """
    name_definition = "Current name of the site"
    name_embedding = model.encode(name_definition)

    # Selects columns that has 'string' type attribute
    pl_string_data = pl_mineralsite.select(
        ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    )
    list_string_based_attributes = pl_string_data.columns


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

    return pl_mineralsite

def identify_geolocation_attribute(pl_mineralsite, dict_known_mappings=None):
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

    dict_known_mappings.update(dict_geolocation_attributes)

    return pl_mineralsite

def identify_textlocation_attribute(pl_mineralsite, known_mappings=None):
    """
    Identifies the attribute that is representing the country and/or state of the mineral site

    : param: pl_mineralsite: polars dataframe of all the records from the mineral site data
    : param: known_mappings (default=None) = 
    : return: pl_mineralsite: polars dataframe with modified attribute labels
    """
    dict_textlocation_attributes = {}

    for i in list(pl_mineralsite.columns):
        if i.lower() == 'country':
            dict_textlocation_attributes[i] = 'country'
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_textlocation_attributes[i] = 'state_or_province'

    return pl_mineralsite

# TODO: Update to include these three
def identify_commodity_attribute(pl_mineralsite, known_mappings=None):
    """
    Identifies attribute(s) representing the commodity available at the mineral site

    : param: pl_mineralsite:
    : param: known_mappings (default=None) = 
    : return: 
    """
    commodity_definition = "Commodities available at the mineral site"
    commodity_embedding = model.encode(commodity_definition)

    return 0

def identify_operationtype_attribute(pl_mineralsite, known_mappings=None):
    """
    
    """
    return 0

def identify_recordyear_attribute(pl_mineralsite, known_mappings=None):
    """
    
    """
    return 0

####

def map_attribute_labels(pl_mineralsite):
    """
    
    """
    dict_known_attribute_maps = open_local_files(path_params['PATH_RESOURCE_DIR'], 'attribute_dictionary', '.pkl')
    known_attribute_label = list(dict_known_attribute_maps.keys())
    known_attribute_map = list(dict_known_attribute_maps.values())

    # map attribute label representing unique id as 'record_id'

    # map attribute label reprepresenting site name as 'name'

    # map attribut elabel representing other name as 'other_names'

    # map attribute label representing longitude and latitude as 'location'

    # identify available attribute labels representing textual location


    pl_processed_mineralsite = pl_mineralsite
    dict_attribute_map = {}

    # Update the stored dictionary

    return pl_processed_mineralsite