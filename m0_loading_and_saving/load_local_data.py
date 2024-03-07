import os
import pickle
import configparser

import regex as re
import polars as pl
import geopandas as gpd

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']

def prompt_user_for_source_name(original_filename:str, estimated_source_name:str):
    """
    Prompts the user to check the source name of the input data

    : param: original_filename = original filename including its extension
    : param: estimated_source_name = estimated source name (i.e., filename without the extension)
    : return: actual_source_name = actual source name (either the estimated name or the newly inputted name of the source)
    """
    print(f'Is \'{estimated_source_name}\' an appropriate source name of \'{original_filename}\'?')
    actual_source_name = input("Enter Y for yes, or a different source name: ").upper()

    if actual_source_name == 'Y':
        actual_source_name = estimated_source_name

    return actual_source_name

def extract_definition_from_df(pl_original_dictionary):
    """
    Extracts the column representing the label and defintion from the data dictionary

    : param: pl_dictionary = polars dataframe of the original data dictionary
    : return: dict_attribute = dictionary that maps the attribute label to the attribution defintion
    """
    dictionary_columns = pl_original_dictionary.columns

    column_label = ''
    column_definition = ''

    for i in dictionary_columns:
        if re.search('short', i.lower()):
            short_definition = i
        elif re.search('descri', i.lower()) or re.search('defin', i.lower()):
            column_definition = i
        elif re.search('label', i.lower()) or re.search('name', i.lower()):
            column_label = i

    pl_extracted_dictionary = pl_original_dictionary.select(
        label = pl.col(column_label).str.strip_chars(),
        definition = pl.col(column_definition).str.strip_chars(),
    )

    dict_attribute = dict(zip(pl_extracted_dictionary['label'], pl_extracted_dictionary['definition']))

    return dict_attribute

def open_local_files(path_directory:str, file_name:str, file_extension:str):
    """
    Reads the local file and converts it to a polars dataframe

    : param: path_directory = directory where the raw data files are located in
    : param: file_name = name of the raw data file
    : param: file_extension = type/extension of the data file (e.g., .csv, .xlsx, .json)
    : return: pl_data = polars dataframe of raw mineral site rec ord
    """
    path_file = os.path.join(path_directory, file_name+file_extension)

    if file_extension == '.csv':
        pl_data = pl.read_csv(path_file, encoding='utf8-lossy')
    
    elif file_extension == '.pkl':
        with open(path_file, 'rb') as handle:
            pl_data = pickle.load(handle)
    
    elif file_extension == '.xls' or file_extension == 'xlsx':
        pl_data = pl.read_excel(
            source=path_file,
            engine='openpyxl',
        )

    elif file_extension == '.json':
        pl_data = pl.read_json(path_file)

    elif file_extension == '.gdb':
        gpd_data = gpd.read_file(
            path_file, 
            driver="OpenFileGDB"
        )
        # TODO: compress down to a polars file by removing the geometry information
        pl_data = gpd_data
    
    return pl_data

def open_local_directory(path_directory:str):
    """
    Opens the local directory that is passed in as part of the input and stores all the files as pickle file in the checkpoint directory
    Location of the temporary directory can be controlled by modifying the 'PATH_CHECKPOINT_DIR' of the params file

    : param: path_directory = directory where the raw data files are located
    : return: list_mineralsite_sources = list of all the mineral site database sources
    """
    # Create a subdirectory to store the raw databases
    path_raw_data_directory = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'raw')
    if not os.path.exists(path_raw_data_directory):
        os.makedirs(path_raw_data_directory)

    # list_unique_source_names = []       # TODO: Make it so that the pipeline can deal with multiple datasets that have the same source name
    list_directory_items = os.listdir(path_directory)

    list_mineralsite_sources = []
    for i in list_directory_items:
        file_name, file_extension = os.path.splitext(i)

        # TODO: Deal with case where there is nested directories

        if re.search('dict', file_name):
            source_name = re.sub('dict|[^A-Za-z0-9]', '', file_name)
            file_name = 'dict_' + source_name

            # If the term 'dict' is available in the file_name it is considered as a dictionary file and stored as a python dictionary type in the checkpoint folder
            pl_dictionary = open_local_files(path_directory, file_name, file_extension)
            dict_attribute = extract_definition_from_df(pl_dictionary)

            with open(os.path.join(path_raw_data_directory, file_name+'.pkl'), 'wb') as handle:
                pickle.dump(dict_attribute, handle, protocol=pickle.HIGHEST_PROTOCOL)
            continue
        
        pl_mineralsite = open_local_files(path_directory, file_name, file_extension)
        source_name = prompt_user_for_source_name(file_name+file_extension, file_name)
        list_mineralsite_sources.append(source_name)
        
        with open(os.path.join(path_raw_data_directory, source_name+file_extension), 'wb') as handle:
            pickle.dump(pl_mineralsite, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return list_mineralsite_sources