import os
import time
import logging
from datetime import datetime

import pickle
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

device = 'cuda' if torch.cuda.is_available() else 'cpu'

def fusemine(args):
    create_directory('./logs/')

    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, 
                        handlers=[
                            logging.FileHandler(f'logs/fusemine_{datetime.timestamp(datetime.now())}.log'),
                            logging.StreamHandler()
                        ])
    logging.info(f'FuseMine is running on {device}')

    if args.single_stage and (args.intralink or args.interlink):
        logging.error(f'Must select either single_stage or two_stage (intralink, interlink). Cannot select both.')
        return -1
    
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

    methods = [intralink_location, ['site_name', 'commodity'], interlink_location, ['site_name', 'commodity']]

    output_directory = args.same_as_directory
    if not output_directory:
        output_directory = './output'

    output_file_name = args.same_as_filename
    if not output_file_name:
        output_file_name = f'{focus_commodity}_sameas'

    logging.info(f'Loading MinMod knowledge graph data for {focus_commodity}')
    start_time = time.time()

    pl_data = load_minmod_kg(focus_commodity)
    pl_data.write_csv(f'./{focus_commodity}_datafile.csv')
    
    if pl_data.is_empty():
        logging.info(f'Program ending due to missing data')
        return -1

    pl_data = pl_data.drop_nulls(subset=['location', 'crs'])
    logging.info(f'{pl_data.shape[0]} records loaded - Elapsed Time: {time.time() - start_time}s')

    pl_data = split_str_column(pl_data, 'site_name').explode('site_name')

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

                logging.info(f'\t{source_id}')
                # logging.info(f'\t{source_id} - {pl_data.shape[0]} records')

                list_pl_by_crs = pl_data.partition_by('crs')

                for pl_crs in list_pl_by_crs:
                    try:
                        pl_crs = compare_geolocation(pl_crs, source_id, methods[0])
                        pl_crs = pl_crs.with_columns(
                            link_method = pl.lit('GEO')
                        )

                    except:
                        try:
                            pl_crs = compare_textual_location(pl_crs, source_id)
                            pl_crs = pl_crs.with_columns(
                                link_method = pl.lit('TXT')
                            )
                        except:
                            logging.info(f'\t\tSkipping location based linking due to missing or incorrect geolocation and textual location')
                            continue

                pl_data = pl.concat(list_pl_by_crs, how='diagonal_relaxed')

                try:
                    pl_crs = compare_text_embedding(pl_crs, source_id, methods[1])
                except:
                    logging.info(f'\t\tSkipping text based linking due to missing textual information')
                    pass

                pl_crs = merge_grouping_results(pl_crs, source_id)
                list_grouped.append(pl_crs)

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

            # pl_data.write_csv('./before_anything.csv')

            pl_loc = pl_data.filter(
                pl.col('link_method') == 'GEO'
            )
            pl_txt = pl_data.filter(
                pl.col('link_method') == 'TXT'
            ).drop(['latitude', 'longitude'])

            if not pl_loc.is_empty():
                pl_loc = compare_geolocation(pl_loc, 'ALL', method=methods[2])
                pl_loc = compare_text_embedding(pl_loc, 'ALL', items_to_compare=methods[3])
                pl_loc = merge_grouping_results(pl_loc, 'ALL').drop(['latitude', 'longitude'])

            pl_data = pl.concat(
                [pl_loc, pl_txt],   
                how='diagonal'
            ).drop(['location']).with_columns(
                loc_GroupID = pl.col('GroupID')
            )

            del pl_loc, pl_txt

            pl_data = compare_textual_location(pl_data, 'GROUP')
            pl_data = compare_text_embedding(pl_data, 'GROUP', items_to_compare=methods[3])
            pl_data = merge_grouping_results(pl_data, 'GROUP', condition='True')

            logging.info(f'Interlinking completed - Elapsed Time: {time.time() - start_time}s')

    try:
        as_csv(pl_data, output_directory, f'{output_file_name}', True)
    except:
        logging.info(f'Failed to save sameas links')

    # --------- Data Quality Check --------- #
    pl_raw_data = pl.read_csv(f'./{focus_commodity}_datafile.csv')
    pl_data = pl_data.select(
        pl.col(['ms_uri', 'GroupID'])
    )
    pl_data.write_csv('./tungsten_grouped.csv')

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

        pl_ground_truth.write_csv('./ground_truth.csv')

        pl_prediction = pl_data.filter(
            pl.col('eval_uri').is_in(list_source_overlap)
        ).sort('eval_uri').select(
            record_id = pl.col('eval_uri'),
            GroupID = pl.col('GroupID')
        )
        pl_prediction.write_csv('./prediction.csv')

        print_evaluation_table('ver 0.2', pl_ground_truth, pl_prediction)

def main():
    parser = argparse.ArgumentParser(description='Linking mineral sites within a database and across databases')

    parser.add_argument('--commodity',
                        help='Specific commodity to focus on')

    parser.add_argument('--single_stage',
                        help='Method for location-based single-stage linking')

    parser.add_argument('--intralink', 
                        help='Method for location-based intralinking')

    parser.add_argument('--interlink',
                        help='Method for location-based interlinking')

    parser.add_argument('--same_as_directory', default='./output',
                        help='Directory to store the same as CSV files (default: ./output)')

    parser.add_argument('--same_as_filename',
                        help='Filename of the same as CSV file (recommended: ./<commodity>_sameas.csv)')

    parser.add_argument('--tungsten', action='store_true', 
                        help='Run evaluation with tungsten')

    args = parser.parse_args()
    fusemine(args)

if __name__ == '__main__':
    main()