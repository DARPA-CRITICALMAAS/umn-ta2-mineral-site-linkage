import os
import time
import logging
from datetime import datetime

import ast
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

        focus_commodity = 'tungsten'

    elif args.subset_file:
        bool_singlestage = False
        bool_intralink = False
        bool_interlink = True

        intralink_location = 'distance'
        interlink_location = 'distance'

    else:
        bool_singlestage = True if args.single_stage else False
        bool_intralink = True if args.intralink else False
        bool_interlink = True if args.interlink else False        

        intralink_location = args.intralink
        interlink_location = args.interlink
        focus_commodity = args.commodity

    intralinked_file = None

    methods = [intralink_location, ['site_name', 'commodity'], interlink_location, ['site_name']]

    output_directory = args.same_as_directory
    if not output_directory:
        output_directory = './output'

    output_file_name = args.same_as_filename
    if not output_file_name:
        try:
            output_file_name = f'{focus_commodity}_sameas'
        except:
            output_file_name = 'output'

    subset_uris = []
    list_grouped = []

    if args.subset_file:        
        try:
            pl_subset = initiate_load(args.subset_file)
            pl_subset = add_index_columns(pl_subset, index_column_name='ms_uri').with_columns(
                pl.col('ms_uri').cast(pl.Utf8)
            )

            logging.info(f'Loaded subset file at {args.subset_file}')
        except:
            logging.info(f'Cannot load subset file. Ending program.')
            return -1

        try:
            pl_subset_map = initiate_load(args.subset_map)
            pl_subset_map = pl_subset_map.drop_nulls(subset=['corresponding_attribute_label'])
            logging.info(f'Using subset attribute map at {args.subset_map}')
        except:
            logging.info('Cannot locate subset attribute map. Ending program.')
            return -1
        
        try:
            pl_kg = initiate_load(args.kg_file)
            pl_kg = add_index_columns(pl_kg, index_column_name='uri').with_columns(
                ms_uri = pl.lit('KG_') + pl.col('uri').cast(pl.Utf8)
            )

            pl_data_map = initiate_load('./resource/dict_kg_map.pkl')

            pl_data = pl_kg.rename(pl_data_map).select(
                pl.col('ms_uri'),
                pl.col(list(pl_data_map.values())),
                # pl.when(pl.col('location').is_null()).then(pl.lit('GEO')).otherwise(pl.lit('TXT')).alias('link_method'),
                link_method = pl.lit('GEO'),
                crs = pl.lit('EPSG:4326'),
                source_id = pl.lit('KG')
            ).drop('uri').drop_nulls('location')
            
            logging.info(f'Loaded KG file at {args.kg_file}')
        except:
            logging.info('Cannot locate KG file. Ending program.')
            return -1

        pl_subset = pl_subset.select(
            pl.col('ms_uri'),
            pl.col(pl_subset_map['corresponding_attribute_label'].to_list()),
            crs = pl.lit('EPSG:4326'),
            source_id = pl.lit('SUBSET')
        )

        dict_map = dict(zip(pl_subset_map['corresponding_attribute_label'].to_list(), pl_subset_map['attribute_label'].to_list()))
        pl_subset = pl_subset.rename(dict_map).with_columns(
            link_method = pl.lit('GEO')
        )
        pl_subset = to_polars(to_geopandas(pl_subset, 'pl'), 'gpd')

        del dict_map

        list_grouped = [pl_data, pl_subset]

        subset_uris = pl_subset['ms_uri'].to_list()
        interlink_location = 'distance'

    else:
        logging.info(f'Loading MinMod knowledge graph data for {focus_commodity}')
        start_time = time.time()

        pl_data = load_minmod_kg(focus_commodity)
        
        if pl_data.is_empty():
            logging.info(f'Program ending due to missing data')
            return -1

        pl_data = pl_data.drop_nulls(subset=['location', 'crs'])
        logging.info(f'{pl_data.shape[0]} records loaded - Elapsed Time: {time.time() - start_time}s')

    if args.tungsten:
        # Filtering out USMIN Tungsten and MRDS Tungsten for evaluation prupose
        pl_data = pl_data.filter(
            (pl.col('source_id') == 'https://mrdata.usgs.gov/mrds') | (pl.col('source_id') == 'https://mrdata.usgs.gov/deposit')
        )

    # ------ Running Single Stage ------ #
    if bool_singlestage:
        pl_data = compare_geolocation(pl_data, method=methods[0])
        pl_data = compare_text_embedding(pl_data, 'ALL', method=methods[1])
        pl_data = merge_grouping_results(pl_data, 'ALL')

    else:
        # ------ Data Separation ------ #
        if not args.subset_file:
            list_pl_by_source = pl_data.partition_by('source_id')

        # --------- Intralink --------- #
        if bool_intralink:
            logging.info(f'Intralinking...')
            intralink_start_time = time.time()
            start_time = time.time()

            for idx, pl_data in enumerate(list_pl_by_source):
                source_id = pl_data.item(0, 'source_id')

                logging.info(f'\t{source_id}')

                list_pl_by_crs = pl_data.partition_by('crs')
                list_pls = []

                for pl_crs in list_pl_by_crs:
                    try:
                        pl_crs = compare_geolocation(pl_crs, source_id, methods[0])
                        pl_crs = pl_crs.with_columns(
                            link_method = pl.lit('GEO')
                        )
                        list_pls.append(pl_crs)

                    except:
                        try:
                            pl_crs = compare_textual_location(pl_crs, source_id)
                            pl_crs = pl_crs.with_columns(
                                link_method = pl.lit('TXT')
                            )
                            list_pls.append(pl_crs)
                        except:
                            logging.info(f'\t\tSkipping location based linking due to missing or incorrect geolocation and textual location')
                            continue

                pl_data = pl.concat(list_pls, how='diagonal_relaxed')

                try:
                    pl_data = compare_text_embedding(pl_data, source_id, methods[1])
                except:
                    logging.info(f'\t\tSkipping text based linking due to missing textual information')
                    pass

                if source_id == 'https://mrdata.usgs.gov/mrds':
                    pl_data.write_csv('./mrds.csv')

                pl_data = merge_grouping_results(pl_data, source_id)
                list_grouped.append(pl_data)

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

    if args.subset_file:
        grp_subset = pl_data.filter(pl.col('ms_uri').is_in(subset_uris))['GroupID'].to_list()
        uri_subset = pl_data.filter(
            (pl.col('GroupID').is_in(grp_subset)) & (pl.col('ms_uri').str.contains('KG'))
        )['ms_uri']

        pl_data = pl_kg.filter(
            pl.col('ms_uri').is_in(uri_subset)
        )

        del grp_subset, uri_subset

        as_csv(pl_data, output_directory, f'{output_file_name}', False)
        return 0

    pl_tmp = pl_data.select(
        pl.col(['ms_uri', 'GroupID'])
    )

    try:
        as_csv(pl_data, output_directory, f'{output_file_name}', True)
    except:
        logging.info(f'Failed to save sameas links')

    # --------- Data Quality Check --------- #
    # pl_raw_data = pl.read_csv(f'./{focus_commodity}_datafile.csv')
    # pl_data = pl_data.select(
    #     pl.col('ms_uri').str.split(','),
    #     pl.col('GroupID')
    # ).explode('ms_uri')

    # pl_data = pl.concat(
    #     [pl_data, pl_raw_data],
    #     how='align'
    # ).filter(
    #     pl.col('location') != ""
    # )
    # del pl_raw_data

    # gpd_output = to_geopandas(pl_data, 'pl', geometry_column='location')
    # del pl_data

    # pl_data.write_csv(f'./{focus_commodity}_grouped.csv')

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
    
    parser.add_argument('--subset_file',
                        help='Target subset to be idenfied in the KG')

    parser.add_argument('--subset_map',
                        help='Map columns of subset file to the KG format')
    
    parser.add_argument('--kg_file',
                        help='File representing data on the KG')

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