import os
import time
import logging
import argparse
import configparser

from utils.load_files import *
from utils.load_kg_data import *
from utils.save_files import *
from utils.dataframe_operations import *
from utils.combine_grouping_results import * 
from utils.compare_geolocation import *
from utils.compare_text import *

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']
prefixes = config['mapping.prefix']

def main(args):
    ##### args statement #####
    bool_data_process = True
    bool_onestage = True
    bool_intralink = False
    bool_interlink = False

    save_intralinked = True

    method_location = 'point'
    item_text = ['name', 'commodity']
    pl_data = None

    dictionary_file_directory = 'dictionary_directory'
    output_directory = 'output_directory'
    output_file = 'output_file'
    intralinked_directory = './path/'
    ######################

    # Data processing to upload it to KG
    if bool_data_process:
        pl_rawdata = initiate_load(args.raw_data, False)
        dict_attribute_map = initiate_load(args.map_file, True, 'corresponding_attribute_label', 'attribute_label')
        pl_mapped_data = map_attribute_labels(pl_rawdata, dict_attribute_map)

        pl_value_map = initiate_load(path_params['PATH_COMMODITY_MAP_FILE'], False)
        pl_mapped_data = map_values(pl_rawdata=pl_mapped_data, 
                                    pl_value_map=pl_value_map, 
                                    column_to_map='commodity', 
                                    store_original_value = 'observed_commodity',
                                    value_map_from=['CommodityinMRDS', 'CodeinMRDS'], 
                                    value_map_to='minmod_id',
                                    prefix=prefixes['MINMOD_PREFIX'])
        as_json(pl_mapped_data, './', 'output')

    pl_data = load_kg()
    available_sourcenames = pl_data.unique(subset=['source_id'], keep='first')['source_id'].to_list()
    dict_attribute_dictionary = load_directory(intralinked_directory, bool_asdict=True, list_target_filename=available_sourcenames)
    # map attributes for those that have the attribute dictionary available
    for source, dictionary_items in dict_attribute_dictionary.items():
        pl_source = pl_data.filter(
            pl.col('source_id') == source
        )
        pl_source = map_attribute_labels(pl_source, dictionary_items)

        pl_remaining = pl_data.filter(
            pl.col('source_id') != source
        )

        pl_data = pl.concat(
            [pl_source, pl_remaining],
            how='align'
        )

        del pl_source, pl_remaining


    # One Stage
    if bool_onestage:
        list_pl_linked = []
        if method_location:
            list_pl_linked.append(compare_geolocation([pl_data], method_location))
        if item_text:
            list_pl_linked.append(compare_text_value_embedding(list_pl_data = [pl_data], 
                                                               items_to_compare=item_text))

        if list_pl_linked > 1:
            pl_data = merge_grouping_results(list_pl_linked[0], list_pl_linked[1])
        else:
            pl_data = list_pl_linked[0]

    # Two Stage (Intralink, Interlink)
    # TODO: Load data in CDR?
    # TODO: Check if a data dictionary exists for each source
    # TODO: structure of data directory
    # target_attribute_dictionary = 
    # list_attribute_dictionary = load_directory(path_intralinked, bool_asdict=True, list_target_filename=available_sourcenames)
    # for db_attribute_dictionary in list_attribute_dictionary:
    #     compare_attribute_embedding(target_attribute_dictionary, db_attribute_dictionary:dict)

    partitioned_pl_data = pl_data.partition_by('source_id')
    # Intralinking
    if bool_intralink:
        if method_location:
            partitioned_pl_data = compare_geolocation(partitioned_pl_data, method_location)
        if item_text:
            partitioned_pl_data = compare_text_value_embedding(list_pl_data = partitioned_pl_data,
                                   items_to_compare=item_text,
                                   orientation='row')

    if save_intralinked:
        # Saves intralinked data to the indicated intralinked directory
        to_directory(partitioned_pl_data, intralinked_directory)

    # Interlinking
    if bool_interlink:
        if intralinked_directory:
            partitioned_pl_data = load_directory(intralinked_directory)

        pl_data = pl.concat(
            partitioned_pl_data,
            how='vertical_relaxed'
        )

        if method_location:
            partitioned_pl_data = compare_geolocation([pl_data], method_location, bool_select_max=True)
        if item_text:
            partitioned_pl_data = compare_text_value_embedding([pl_data], item_text)

    # Saving linked results
    as_csv(partitioned_pl_data[0], output_directory, output_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    subparsers = parser.add_subparsers(dest='subcommand')
    subparsers.required = True  # required since 3.7

    parser_dataprocess = subparsers.add_parser('data_process')
    parser_dataprocess.add_argument('--raw_data', '-d',
                                    help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser_dataprocess.add_argument('--map_file', '-m',
                                    help='CSV file where the mapping information is stored')

    
    # parser.add_argument('--one_stage', nargs='+', choices = ['point', 'polygon', 'name', 'commodity'])
    # parser.add_argument('--intralink', nargs='+', choices = ['point', 'polygon'])
    # parser.add_argument('--interlink', nargs='+', choices = ['point', 'polygon', 'name', 'commodity'])


    # parser_link = subparsers.add_parser('link')
    # parser_link.add_argument('stage_choice', choices=['one_stage', 'intralink', 'interlink'])

    # parser.add_argument('--raw_data', '-d',
    #                     help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    # parser.add_argument('--map_file', '-f',
    #                     help='CSV file where the mapping information is stored')
    
    parser.add_argument('--output_filename', '-o',
                        help='final output filename of the interlinked result')
    # parser.add_argument('--use_location_base', '-l',
    #                     help='use location based method to link data', action='store_true')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    
    main(parser.parse_args())