import os
import time
import logging
import argparse
import configparser

from utils.load_files import *
from utils.load_kg_data import *
from utils.save_files import *
from utils.dataframe_operations import *
from utils.compare_geolocation import *
from utils.compare_text import *

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']
prefixes = config['mapping.prefix']

def main(args):
    ##### PLACEHOLDER #####
    bool_data_process = True
    bool_onestage = True
    bool_intralink = False
    bool_interlink = False

    method_location = 'point'
    item_text = ['name', 'commodity']
    pl_data = None
    ######################

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
        # as_json(pl_mapped_data, './', 'output')

    pl_data = load_kg()
    available_sourcenames = pl_data.unique(subset=['source_id'], keep='first')['source_id'].to_list()

    # TODO: Load data in CDR?
    # TODO: Check if a data dictionary exists for each source
    # TODO: structure of data directory

    # One Stage
    if bool_onestage:
        if method_location:
            pl_data = compare_geolocation([pl_data], method_location)
        if item_text:
            pl_data = compare_text(list_pl_data = [pl_data], 
                                   items_to_compare=item_text,
                                   orientation='row')

    # Two Stage (Intralink, Interlink)

    # TODO: If data dictionary exists, try to find unidentified column names

    partitioned_pl_data = pl_data.partition_by('source_id')
    # Intralinking
    if bool_intralink:
        if method_location:
            pl_data = compare_geolocation(partitioned_pl_data, method_location)
        if item_text:
            pl_data = compare_text(list_pl_data = partitioned_pl_data, # oRIGINAL
                                   items_to_compare=item_text,
                                   orientation='row')

        # IF BOTH COMBINE
            
    # Interlinking
    if bool_interlink:
        # TODO: laod intralinked temporary file or use the result from intralinking. if both do not exist, throw error and end program
        # tAKE IN INTRALINKED DIRECTORY
        if method_location:
            compare_geolocation(pl_data, method_location, bool_select_max=True)
        if item_text:
            compare_text(pl_data, item_text)

    # Saving linked results

    return 0

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