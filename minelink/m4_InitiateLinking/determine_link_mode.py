from minelink.params import *
from minelink.m0_SaveAndLoad.save_load_directory import *
from minelink.m1_PreProcessing.dataframe_preprocessing import separate_dataframe
from minelink.m4_InitiateLinking.intra_linking import *
from minelink.m4_InitiateLinking.inter_linking import *

def site_linking(path_data, bool_location=False):
    check_dir(PATH_TMP_DIR)

    list_files, len_files, list_dict = open_dir(path_data, bool_dict=True)

    dict_tmp_alias = {}
    leading_char = 'a'
    follow_char = 'a'

    for l in list_files:
        file_name, file_extension = os.path.splitext(l)

        if file_extension in ACCEPTABLE_INPUT_FILE:
            try:
                source_name = re.sub('_', '/', file_name)
            except:
                source_name = file_name

            tmp_file_folder = os.path.join(PATH_TMP_DIR, leading_char+follow_char)
            check_dir(tmp_file_folder)

            dict_tmp_alias[leading_char+follow_char] = source_name

            df = load_file(path_data, file_name, file_extension)
            dump_file(df, tmp_file_folder, 'raw', 'PICKLE')

            dict_sameas, dict_loc, dict_geo, df_link = separate_dataframe(df, leading_char+follow_char, source_name)
            dump_file(dict_sameas, tmp_file_folder, 'same_as', 'PICKLE')
            dump_file(dict_loc, tmp_file_folder, 'location_info', 'PICKLE')
            dump_file(dict_geo, tmp_file_folder, 'geometry', 'PICKLE')

            # TODO: intra link on the df link
            dump_file(df_link, tmp_file_folder, 'df_link', 'PICKLE')

            follow_char = chr(ord(follow_char) + 1) 
            leading_char = chr(ord(leading_char) + 1) if follow_char == 'a' else leading_char

    dump_file(dict_tmp_alias, PATH_TMP_DIR, 'code_alias', 'PICKLE')

    # for i in list_dir_items:
    #     # TODO: run intra linking

    # if len_items != 1:
    #     # TODO: pass in the dataframes to inter