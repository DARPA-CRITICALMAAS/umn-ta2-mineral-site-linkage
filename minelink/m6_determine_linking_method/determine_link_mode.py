import os

from minelink.params import *
from minelink.m0_save_and_load.save_load_directory import *
from minelink.m0_save_and_load.save_load_file import *
from minelink.m1_preprocessing.dataframe_preprocessing import separate_dataframe
from minelink.m1_preprocessing.datadictionary_processing import *
from minelink.m4_intralinking.intra_linking import *
from minelink.m5_interlinking.inter_linking import *

def sort_dictionary_files(path_data, list_dict):
    check_dir(PATH_TMP_DIR, 'dictionary')

    list_dict_names = []

    for l in list_dict:
        file_name, file_extension = os.path.splitext(l)

        try:
            source_name = re.split('/', file_name)[1]
        except:
            source_name = file_name

        if re.search('dict_', source_name):
            source_name = re.split('dict_', source_name)[1]

        list_dict_names.append(source_name)

        df = load_file(path_data, file_name, file_extension)
        df_description = df_to_dictionary(df)

        dump_file(df_description, os.path.join(PATH_TMP_DIR, 'dictionary'), source_name, 'PICKLE')

    return list_dict_names

def sort_data_files(path_data):
    check_dir(PATH_TMP_DIR)

    list_files, len_files, list_dict = open_dir(path_data, bool_dict=True)
    list_dict_names = sort_dictionary_files(path_data, list_dict)

    dict_tmp_alias = {}
    leading_char = 'a'
    follow_char = 'a'

    for l in list_files:
        file_name, file_extension = os.path.splitext(l)

        if file_extension in ACCEPTABLE_INPUT_FILE:
            tmp_file_folder = os.path.join(PATH_TMP_DIR, leading_char+follow_char)
            check_dir(tmp_file_folder)

            try:
                source_name = re.sub('_', '/', file_name)
            except:
                source_name = file_name

            dict_tmp_alias[leading_char+follow_char] = source_name

            if file_name in list_dict_names:
                move_file(os.path.join(PATH_TMP_DIR, 'dictionary', file_name+'.pkl'), 
                          os.path.join(tmp_file_folder, 'dictionary.pkl'))

            df = load_file(path_data, file_name, file_extension)
            df_tolink = separate_dataframe(df, tmp_file_folder, leading_char+follow_char, source_name)

            # TODO: intra link on the df link
            df_links = intra_link(df_tolink, tmp_file_folder)

            follow_char = chr(ord(follow_char) + 1) 
            leading_char = chr(ord(leading_char) + 1) if follow_char == 'a' else leading_char

    dump_file(dict_tmp_alias, PATH_TMP_DIR, 'code_alias', 'PICKLE')

    remove_dir(additional='dictionary')

    return list(dict_tmp_alias.keys())

def site_linking(path_data, bool_location=False):
    list_files = sort_data_files(path_data)

    if len(list_files) == 1:
        df_links = load_file(os.path.join(PATH_TMP_DIR, list_files[0]), 'df_links', '.pkl')
    else:
        inter_linking(list_files)

    dump_file(df_links, PATH_TMP_DIR, 'df_links', 'PICKLE')

    