import configparser
import numpy as np
from sentence_transformers import SentenceTransformer, util

from m0_loading_and_saving.load_local_data import open_local_files

config = configparser.ConfigParser()
config.read('../params.ini')
ATTRIBUTE_DEF_SIMILARITY_THRESHOLD = float(config['preprocessing.params']['ATTRIBUTE_DEFINITION_THRESHOLD'])

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

def find_similar_attributes(attribute_defintion, dict_attributes):
    """
    
    : param: attribute_definition = definition of attribute defined by us (the definition of the target attribute we are trying to identify)
    : param: dict_attributes = dictionary that gives label and its definition
    : return: set_potential_matching_attributes = set type including attributes that are considered to have similar definition with the input 'attribute_definition'
    """
    attribute_embedding = model.encode(attribute_defintion, convert_to_tensor=True)

    label_potential_matching_attribute = list(dict_attributes.keys())
    potential_matching_attribute_definition = list(np.array(list(dict_attributes.values())).flatten())
    potential_matching_attribute_embedding = model.encode(potential_matching_attribute_definition, convert_to_tensor=True)

    attribute_cosine_similarity = util.cos_sim(attribute_embedding, potential_matching_attribute_embedding)
    attribute_cosine_similarity = np.array(attribute_cosine_similarity.cpu())

    # Get the indexes where the attribute similarity score is over 0.45
    idx = list(dict.fromkeys(np.where(attribute_cosine_similarity > ATTRIBUTE_DEF_SIMILARITY_THRESHOLD)[1]))

    set_potential_matching_attributes = set(np.array(label_potential_matching_attribute)[idx])

    return set_potential_matching_attributes