import os
import time
import logging
from datetime import datetime

import argparse
import configparser

import regex as re
# Temporary helpers
import pandas as pd
import polars as pl
import pickle
#

from utils.load_files import *
from utils.load_kg_data import *
from utils.compare_geolocation import *
from utils.compare_text import *

from utils.convert_dataframe import *
from utils.combine_grouping_results import *
from utils.save_files import *

from process_rawdata import process_rawdata

logging.basicConfig(filename=f'fusemine_{datetime.timestamp(datetime.now())}.log', format='%(asctime)s: %(message)s', level=logging.INFO)

def fusemine(args):
    path_rawdata = None
    path_CDR = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA'
    path_attribute_map = None
    path_output_dir = None
    path_filename = None

    bool_singlestage = False
    methods = ['distance', ['site_name', 'commodity'], 'area', None]
    use_previous_sameas = True
    bool_intralink = True
    intralinked_file = None
    bool_interlink = True
    output_directory = '/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/sameas' # args.same_as_directory
    output_file_name = 'cobalt_results' # args.same_as_output
    focus_commodity = 'cobalt' # args.commodity
    # if not args.commodity:
    #     focus_commodity = 'ALL'

    if path_rawdata:
        logging.info(f'Processing data at {path_rawdata} to suggested mineral site schema')

        try:
            process_rawdata(path_rawdata, path_attribute_map, path_output_dir, path_filename)
        except:
            if not path_attribute_map:
                logging.error(f'Process exiting due to missing attribute_map')
                print('ERROR: Process exiting due to missing')


    logging.info(f'Loading MinMod knowledge graph data for {focus_commodity}')
    start_time = time.time()

    ### temporary ###
    pl_data = load_minmod_kg(focus_commodity).drop_nulls(subset=['location', 'crs']).filter(
        (pl.col('source_id') != 'https://doi.org/10.5382/econgeo.4950') & (pl.col('source_id') != 'https://w3id.org/usgs/z/4530692/3WDZDNIF')
    )

    # pl_mrds = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/TMP/ARCHIVE/data/raw/MRDS.csv').select(
    #     pl.col('dep_id').cast(pl.Utf8).str.strip_chars()
    # )['dep_id'].to_list()

    # pl_usmin = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/TMP/ARCHIVE/data/raw/USMIN.csv').select(
    #     pl.col('Site_ID').str.strip_chars()
    # )['Site_ID'].to_list()

    # pl_mrds.extend(pl_usmin)

    # pl_data = pl_data.filter(
    #     pl.col('record_id').str.strip_chars().is_in(pl_mrds)
    # )
    #################

    logging.info(f'{pl_data.shape[0]} records loaded - Elapsed Time: {time.time() - start_time}s')

    # --------- Appending CDR Data --------- #
    # dict_raw_data_sources = load_directory_names(path_CDR)
    # list_queried_sources = pl_data.unique('source_id')['source_id'].to_list()

    # for source in list_queried_sources:
    #     subbed_source_name = re.sub('[^A-Za-z0-9]', '', source).lower()
    #     if subbed_source_name in list(dict_raw_data_sources.keys()):
    #         path_identified_rawdata = dict_raw_data_sources[subbed_source_name]

    #         data_dictionary = initiate_load(os.path.join(path_identified_rawdata, 'dictionary.csv'))
    #         data_dictionary = string_match_on_attribute(data_dictionary, 'Other Name')

    #         pl_raw_data = load_directory(path_identified_rawdata, list_exclude_filename=['dictionary']).with_columns(
    #             source_id = pl.lit(source)
    #         )
    #         columns_raw_data = list(pl_raw_data.columns)

    #         identified_attribute_label = None
    #         try:
    #             identified_attribute_label = data_dictionary.with_columns(
    #                 identified_labels = pl.struct(pl.all()).map_elements(lambda x: identify_list_items_overlap(x, columns_raw_data))
    #             ).item(0, 'identified_labels')
    #         except:
    #             pass

    #         if identified_attribute_label:
    #             ### temporary ###
    #             try:
    #                 pl_raw_data = pl_raw_data.select(
    #                     pl.col(['source_id', 'Site_ID', identified_attribute_label])
    #                 ).rename({
    #                     identified_attribute_label: 'other_name',
    #                     'Site_ID': 'record_id',
    #                 })
    #             except:
    #                 pl_raw_data = pl_raw_data.select(
    #                     pl.col(['source_id', 'rec_id', identified_attribute_label])
    #                 ).rename({
    #                     identified_attribute_label: 'other_name',
    #                     'rec_id': 'record_id',
    #                 })
                
    #             pl_data = pl.concat(
    #                 [pl_data, pl_raw_data],
    #                 how='align'
    #             ).drop_nulls(
    #                 subset=['record_id', 'location']
    #             ).fill_null('')

    # pl_data = pl_data.rename(
    #     {'site_name': 'name'}
    # ).with_columns(
    #     pl.concat_str(
    #         [
    #             pl.col(['name', 'other_name']),
    #         ],
    #         separator=",",
    #     ).alias("site_name"),
    # ).drop(['name', 'other_name'])

    # pl_data = split_str_column(pl_data, 'site_name', bool_replace_numbers=False).explode('site_name')

    #################

    # ------ Running Single Stage ------ #
    if bool_singlestage:
        pl_data = compare_geolocation(pl_data, method=methods[0])
        pl_data = compare_text_value_embedding(pl_data, method=methods[1])

        pl_data = merge_grouping_results(pl_data, 'ALL')

    else:
        # ------ Data Separation ------ #
        list_pl_by_source = pl_data.partition_by('source_id')
        list_grouped = []

        # --------- Intralink --------- #
        if bool_intralink:
            logging.info(f'Intralinking...')
            start_time = time.time()

            for idx, pl_data in enumerate(list_pl_by_source):
                source_id = pl_data.item(0, 'source_id')

                logging.info(f'\t{source_id}')
                try:
                    pl_data = compare_geolocation(pl_data, source_id, methods[0])
                except:
                    logging.info(f'\t\tSkipping due to missing or incorrect geolocation information')
                    continue
                pl_data = compare_text_value_embedding(pl_data, source_id, methods[1])

                pl_data = merge_grouping_results(pl_data, source_id)

                # pl_data.write_csv(f'./{focus_commodity}_{idx}.csv')
                list_grouped.append(pl_data)

            logging.info(f'Intralinking on {len(list_grouped)} sources completed - Elapsed Time: {time.time() - start_time}s')

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
            pl_data = compare_text_value_embedding(pl_data, methods[3])

            pl_data = merge_grouping_results(pl_data, 'ALL')

            logging.info(f'Interlinking completed - Elapsed Time: {time.time() - start_time}s')

        # print(pl_data)

    as_csv(pl_data, output_directory, output_file_name, True)
    as_csv(pl_data, output_directory, 'output_file_name', False)

if __name__ == '__main__':
    # parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    # parser.add_argument('--raw_data',
    #                     help='the directory or file where the raw mineral site databases are located')
    # parser.add_argument('--map_file',
    #                     help='CSV file where the label mapping information is stored. See sample_mapfile.csv for reference')
    # parser.add_argument('--raw_output',
    #                     help='directory in where you will like to store the processed raw mineral site database')
    # parser.add_argument('--commodity',
    #                     help='specific commodity to focus on (default: no all commodities)')

    # parser.add_argument('--single_stage',
    #                     help='do not separate to intralinking and interlinking', action='store_true')
    # parser.add_argument('--intralink',
    #                     help='perform intralinking: do within database linking', action='store_true')
    # parser.add_argument('--interlink',
    #                     help='perform interlinking: do across database linking', action='store_true')
    
    # parser.add_argument('--same_as_directory',
    #                     help='directory to store the same as CSV files')
    # parser.add_argument('--same_as_output',
    #                     help='filename of the same as CSV file')

    fusemine('one')
    # fusemine(parser.parse_args())