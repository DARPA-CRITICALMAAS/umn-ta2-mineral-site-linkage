import os
import pickle
import polars as pl

def load_mapped_dictionary(path_directory:str, file_name:str) -> dict:
    """
    loads the mapped dictionary given as a csv file and returns the information in the csv file as a dictionary item
    
    : param: path_directory = path (directory) where the mapped dictionary is stored
    : param: file_name = name of the csv file
    : return: dict_attribute_mapped = dictionary object where key is the target attribute and value is the mapped attribute (a.k.a. attribute label that can be found in the mineral site database)
    """
    pl_mapped_dictionary = pl.read_csv(os.path.join(path_directory, file_name+'.csv'))
    
    list_attributes = pl_mapped_dictionary['attribute'].to_list()
    list_mapped_attributes = pl_mapped_dictionary['Matching Attributes in Database'].to_list()

    dict_attribute_mapped = dict(zip(list_attributes, list_mapped_attributes))

    return dict_attribute_mapped
