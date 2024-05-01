import os
import regex as re
import polars as pl

from utils.load_kg_data import *

def load_file(file_name_full:str):
    pl_data = pl.read_csv(file_name_full, ignore_errors=True, truncate_ragged_lines=True)

    return pl_data

def load_directory(directory_path:str, file_exclude:list|None=None):
    files_in_directory = os.listdir(directory_path)

    pl_data_whole = pl.DataFrame()
    for file in files_in_directory:
        file_basename = os.path.splitext(os.path.basename(file))[0]
        file_name, file_extension = os.path.splitext(file)

        if file_exclude and file_name in file_exclude:
            continue

        if not file_extension:
            load_directory(os.path.join(directory_path, file_name))
        else:
            pl_data = load_file(os.path.join(directory_path, file))

            if pl_data_whole.is_empty():
                pl_data_whole = pl_data
            
            else:
                try:
                    pl_data_whole = pl.concat(
                        [pl_data_whole, pl_data],
                        how='align'
                    )
                except:
                    pass

    return pl_data_whole

def list_raw_data_sources(raw_data_path:str):
    items_in_directory = os.listdir(raw_data_path)
    data_directories = {}

    for i in items_in_directory:
        directory_path = os.path.join(raw_data_path, i)

        cleaned_items = re.sub('[^A-Za-z0-9]', '',  i).lower()
        data_directories[cleaned_items] = directory_path
   
    return data_directories

def load_original_data(data_directories:dict, source_name:str) -> str:
    list_directories = list(data_directories.keys())

    try:
        subbed_source_name = re.sub('[^A-Za-z0-9]', '', source_name).lower()

        if subbed_source_name in list_directories:
            raw_data_path = data_directories[subbed_source_name]
            
    except:
        pass

def string_match_identify_columns(pl_data, item:str):
    item = re.sub('[^A-Za-z0-9]', '', item)

    pl_data = pl_data.with_columns(
        pl.col(pl.Utf8).str.strip_chars()
    ).with_columns(
        all_combined = pl.concat_str(
            pl.all().fill_null('').str.replace_all("[^A-Za-z0-9]", "")           
        )
    ).filter(
        pl.col('all_combined').str.contains(rf'(?i){item}')
    )

    return pl_data

def load_file_from_directory(data_directory:str, file_name:str, file_type:str) -> str:
    file_name_full = os.path.join(data_directory, file_name+file_type)
    pl_file = load_file(file_name_full)

    return pl_file

# pl_data = load_minmod_kg('cobalt')

def get_header_value(dict_value:dict, target_list:list):
    dict_values = list(dict_value.values())
    overlapping = list(set(dict_values) & set(target_list))

    return overlapping[0]

# data_directories = list_raw_data_sources('/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA')

# pl_data = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/minmod_cobalt.csv')
# pl_data_sources = pl_data.unique('source_id')['source_id'].to_list()

# for i in pl_data_sources:
#     raw_data_path = load_original_data(data_directories, i)

#     if raw_data_path:
#         data_dictionary = load_file_from_directory(raw_data_path, 'dictionary', '.csv')
#         remaining = string_match_identify_columns(data_dictionary, 'other name')

#         pl_data_whole = load_directory(raw_data_path, ['dictionary'])
#         column_name_of_full = list(pl_data_whole.columns)

#         remaining = remaining.with_columns(
#             correct = pl.struct(pl.all()).map_elements(lambda x: get_header_value(x, column_name_of_full), skip_nulls=False)
#         ).item(0, 'correct')
        
#         print(remaining)

# pl_data = load_minmod_kg('tungsten').filter(
#     (pl.col('source_id') == 'USMIN') | (pl.col('source_id') == 'MRDS')
# )

# print(pl_data)
# pl_data.write_csv('./minmod_tungsten.csv')

# pl_data.write_csv('./tmp.csv')

# .filter(
#     pl.col('record_id').is_in(pl_mrds)
# )

# pl_data.write_csv('minmod_tungsten_partial.csv')