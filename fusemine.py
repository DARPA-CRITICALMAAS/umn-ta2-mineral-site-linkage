import os
import time
import logging
from datetime import datetime

import torch
import argparse
import configparser

import polars as pl

from utils.load_files import *
from utils.append_rawdata import *
from utils.load_kg_data import *
from utils.compare_geolocation import *
from utils.compare_text import *

from utils.convert_dataframe import *
from utils.combine_grouping_results import *
from utils.save_files import *
from utils.performance_evaluation import *
from process_rawdata import process_rawdata

device = 'cuda' if torch.cuda.is_available() else 'cpu'

def fusemine(args):
    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, 
                        handlers=[
                            logging.FileHandler(f'fusemine_{datetime.timestamp(datetime.now())}.log'),
                            logging.StreamHandler()
                        ])
    logging.info(f'FuseMine is running on {device}')

    if args.single_stage and (args.intralink or args.interlink):
        logging.error(f'Must select either single_stage or two_stage (intralink, interlink). Cannot select both.')
        return -1
    
    path_rawdata = args.raw_data
    path_attribute_map = args.attribute_map
    path_output_dir = args.schema_output_directory
    schema_filename = args.schema_output_filename
    
    if args.tungsten:
        # Default option for evaluation on Tungsten
        bool_singlestage = False
        bool_intralink = True
        bool_interlink = True

        intralink_location = 'distance'
        interlink_location = 'area'
        intralinked_file = None

        focus_commodity = 'tungsten'
    else:
        bool_singlestage = True if args.single_stage else False
        bool_intralink = True if args.intralink else False
        bool_interlink = True if args.interlink else False        

        intralink_location = args.intralink or args.single_stage
        interlink_location = args.interlink
        intralinked_file = None

        focus_commodity = args.commodity

    methods = [intralink_location, ['site_name', 'commodity'], interlink_location, None]
    # methods = [intralink_location, intralink_text, interlink_location, None]

    output_directory = args.same_as_directory
    if not output_directory:
        output_directory = './output'

    output_file_name = args.same_as_filename
    if not output_file_name:
        output_file_name = f'{focus_commodity}_sameas'
        
    if path_rawdata:
        logging.info(f'Processing data at {path_rawdata} to suggested mineral site schema')

        try:
            process_rawdata(path_rawdata, path_attribute_map, path_output_dir, schema_filename)
        except:
            if not path_attribute_map:
                logging.error(f'Process exiting due to missing attribute_map')

    logging.info(f'Loading MinMod knowledge graph data for {focus_commodity}')
    start_time = time.time()

    pl_data = load_minmod_kg(focus_commodity).drop_nulls(subset=['location', 'crs'])
    logging.info(f'{pl_data.shape[0]} records loaded - Elapsed Time: {time.time() - start_time}s')

    try:
        pl_data = append_rawdata(pl_data)
        logging.info(f'\tOriginal data source has been found. Additional fields have been appended.')
    except:
        logging.warning(f'\tOriginal data source has not been found. This may lead to a lower performance of FuseMine.')
        pass

    if args.tungsten:
        # Filtering out USMIN Tungsten and MRDS Tungsten for evaluation prupose
        pl_data = pl_data.filter(
            (pl.col('source_id') == 'https://mrdata.usgs.gov/mrds') | (pl.col('source_id') == 'https://mrdata.usgs.gov/deposit')
        )

        pl_data.write_csv('./tungsten.csv')

    # ------ Running Single Stage ------ #
    if bool_singlestage:
        pl_data = compare_geolocation(pl_data, method=methods[0])
        pl_data = compare_text_embedding(pl_data, 'ALL', method=methods[1])

        pl_data = merge_grouping_results(pl_data, 'ALL')

    else:
        # ------ Data Separation ------ #
        list_pl_by_source = pl_data.partition_by('source_id')
        list_grouped = []

        # --------- Intralink --------- #
        if bool_intralink:
            logging.info(f'Intralinking...')
            intralink_start_time = time.time()
            start_time = time.time()

            for idx, pl_data in enumerate(list_pl_by_source):
                source_id = pl_data.item(0, 'source_id')

                logging.info(f'\t{source_id} - {pl_data.shape[0]} records')
                try:
                    pl_data = compare_geolocation(pl_data, source_id, methods[0])
                    pl_data = compare_text_embedding(pl_data, source_id, methods[1])

                    pl_data = merge_grouping_results(pl_data, source_id)
                    list_grouped.append(pl_data)
                    
                except:
                    logging.info(f'\t\tSkipping due to missing or incorrect geolocation information')
                    continue

            logging.info(f'Intralinking on {len(list_grouped)} sources completed - Elapsed Time: {time.time() - intralink_start_time}s')

        # --------- Interlink --------- #
        if bool_interlink:
            if intralinked_file:
                logging.info(f'Loading intralinked file on local')

            logging.info(f'Interlinking...')
            start_time = time.time()

            pl_data = pl.concat(
                list_grouped,
                how='diagonal'
            )

            pl_data = compare_geolocation(pl_data, method=methods[2])
            pl_data = compare_text_embedding(pl_data, methods[3])

            pl_data = merge_grouping_results(pl_data, 'ALL')

            logging.info(f'Interlinking completed - Elapsed Time: {time.time() - start_time}s')

    as_csv(pl_data, output_directory, output_file_name, True)

    # --------- Evaluation --------- #
    if args.tungsten:
        pl_data = pl_data.with_columns(
            pl.col('record_id').str.split(',')
        ).explode(
            'record_id'
        ).with_columns(
            pl.col('source_id').replace({
                'https://mrdata.usgs.gov/mrds': 'MRDS',
                'https://mrdata.usgs.gov/deposit': 'USMIN'
        }))
    
        pl_data = pl_data.with_columns(
            eval_uri = pl.col('source_id') + pl.lit('_') + pl.col('record_id')
        )

        pl_ground_truth = initiate_load('./resource/tungsten_gt.pkl')

        set_ground_truth_sources = set(pl_ground_truth['record_id'].to_list())
        set_data_sources = set(pl_data['eval_uri'].to_list())
        list_source_overlap = list(set_ground_truth_sources & set_data_sources)

        pl_ground_truth = pl_ground_truth.filter(
            pl.col('record_id').is_in(list_source_overlap)
        ).sort('record_id').select(
            pl.col(['record_id', 'GroupID'])
        )

        pl_prediction = pl_data.filter(
            pl.col('eval_uri').is_in(list_source_overlap)
        ).sort('eval_uri').select(
            record_id = pl.col('eval_uri'),
            GroupID = pl.col('GroupID')
        )

        print_evaluation_table('ver 0.2', pl_ground_truth, pl_prediction)

def main():
    parser = argparse.ArgumentParser(description='Linking mineral sites within a database and across databases')

    parser.add_argument('--raw_data',
                        help='Directory or file where the raw mineral site databases are located')

    parser.add_argument('--attribute_map', 
                        help='CSV file with label mapping information (see sample_mapfile.csv for reference)')

    parser.add_argument('--schema_output_directory', 
                        help='Directory where the processed raw mineral site database will be stored')

    parser.add_argument('--schema_output_filename', 
                        help='Filename for the processed raw mineral site database')

    parser.add_argument('--commodity',
                        help='Specific commodity to focus on')

    parser.add_argument('--single_stage',
                        help='Method for location-based single-stage linking (recommended: distance)')

    parser.add_argument('--intralink', 
                        help='Method for location-based intralinking (recommended: distance)')

    parser.add_argument('--interlink',
                        help='Method for location-based interlinking (recommended: area)')

    parser.add_argument('--same_as_directory', default='./output',
                        help='Directory to store the same as CSV files (recommended: ./output)')

    parser.add_argument('--same_as_filename',
                        help='Filename of the same as CSV file (recommended: ./<commodity>_sameas.csv)')

    parser.add_argument('--tungsten', action='store_true', 
                        help='Run evaluation with tungsten')

    args = parser.parse_args()
    fusemine(args)

if __name__ == '__main__':
    main()