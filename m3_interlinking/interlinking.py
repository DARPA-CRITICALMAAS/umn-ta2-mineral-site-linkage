import os
import time
import logging
import pickle
import argparse
import configparser

from m0_loading_and_saving import load_local_data, load_kg_data, save_to_geojson_output, save_to_json_output
from m1_preprocessing.process_rawdb_to_schema import preprocessing_rawdb
from m2_intralinking.intralinking import intralinking
from m3_interlinking.location_based_interlinking import location_based_linking

# Initializing logging file for interlinking
logging.basicConfig(filename='fusemine_interlinking.log', format='%(levelname)s:%(message)s', level=logging.INFO)

config = configparser.ConfigParser()
config.read('../params.ini')
path_params = config['directory.paths']

def interlinking(list_mineralsite_sources, bool_location_based, bool_geojson, output_filename='interlinked'):
    """
    
    : param: list_mineralsite_sources = 
    : param: bool_location_based = 
    : param: bool_geojson = 
    : param: output_filename (default='interlinked') = 
    """
    intralinked_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'intralinked')
    interlinked_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'interlinked')

    for idx in list(range(1, len(list_mineralsite_sources))):
        if idx == 1:
            pl_intralinked_mineralsite1 = load_local_data.open_local_files(intralinked_location, list_mineralsite_sources[0], '.pkl')
        else:
            pl_intralinked_mineralsite1 = pl_interlinked_mineralsite
        pl_intralinked_mineralsite2 = load_local_data.open_local_files(intralinked_location, list_mineralsite_sources[idx], '.pkl')
        
        pl_interlinked_mineralsite = location_based_linking(pl_intralinked_mineralsite1, pl_intralinked_mineralsite2)
        del (pl_intralinked_mineralsite1, pl_intralinked_mineralsite2)

    logging.info(f'\tSaving interlinked data between {list_mineralsite_sources} as JSON file to {intralinked_location}')
    save_to_json_output.save_mineralsite_output_json(pl_interlinked_mineralsite, interlinked_location, output_filename)

    if bool_geojson:
        logging.info(f'\tSaving interlinked data between {list_mineralsite_sources} as GEOJSON file to {intralinked_location}')
        save_to_geojson_output.save_mineralsite_output_geojson(pl_interlinked_mineralsite, interlinked_location, output_filename)

def main(args):
    if args.data_dir:
        list_mineralsite_sources = load_local_data.open_local_directory(args.data_dir)
        preprocessing_rawdb(list_mineralsite_sources, False)
    intralinking(list_mineralsite_sources, args.use_location_base, args.save_as_geojson)

    logging.info(f'Interlinking process started')
    start_time = time.time()

    interlinking(list_mineralsite_sources, args.use_location_base, args.save_as_geojson, args.output_filename)

    logging.info(f'Interlinking process ended. Total run time: {time.time() - start_time}s')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--output_filename', '-o',
                        help='final output filename of the interlinked result')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    
    main(parser.parse_args())