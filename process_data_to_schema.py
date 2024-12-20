import os
import time
import logging
import argparse
import configparser
from datetime import datetime

from utils.load_files import *
from utils.save_files import *
from utils.unify_coordinate_system import *
from utils.dataframe_operations import *

def process_rawdata(args):
    if not os.path.exists('./logs'):
        os.makedirs('./logs')

    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, 
                        handlers=[
                            logging.FileHandler(f'./logs/process_data_to_schema_{datetime.timestamp(datetime.now())}.log'),
                            logging.StreamHandler()
                        ])

    path_rawdata = args.raw_data
    path_attribute_map = args.attribute_map
    path_output_dir = args.schema_output_directory
    path_filename = args.schema_output_filename

    if not os.path.exists(path_output_dir):
        os.makedirs(path_output_dir)

    logging.info(f'Processing data at {path_rawdata} to suggested mineral site schema')

    start_time = time.time()

    try:
        pl_attribute_map = initiate_load(path_attribute_map)
        pl_attribute_map = pl_attribute_map.drop_nulls(subset=['corresponding_attribute_label'])
        logging.info(f'Using attribute map at {path_attribute_map}')
    except:
        logging.info('Cannot locate attribute map. Ending program')
        # logging.info('Cannot locate attribute map. Using attribute label identification to process data. This may lead to incorrect results.')
        # pass
        return -1

    try:
        group_by_column = pl_attribute_map.filter(
            pl.col('attribute_label') == 'record_id'
        ).item(0, 'corresponding_attribute_label')
    except:
        group_by_column = None

    # # list_record_id_archive = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'attribute_archive.pkl'))
    # # list_record_id_archive['record_id'].append(group_by_column)
    # # as_pkl(list_record_id_archive, os.path.join(path_params['PATH_RSRC_DIR'], 'attribute_archive.pkl'))

    # # For the case where the input is a directory
    file_names = pl_attribute_map.unique(
        'file_name'
    )['file_name'].to_list()

    file_names = list(filter(None, file_names))

    if len(file_names) > 1:
        pl_data = load_directory(path_rawdata, file_names, group_by_col=group_by_column)
    else:
        pl_data = initiate_load(path_rawdata, False)

    pl_attribute_map = pl_attribute_map.rename({'corresponding_attribute_label': 'tmp_attribute_label'}).with_columns(
        pl.when((pl.col('file_name').is_null()) | (pl.col('tmp_attribute_label') == group_by_column))
        .then(pl.col('tmp_attribute_label'))
        .otherwise(pl.col('file_name') + pl.lit('_') + pl.col('tmp_attribute_label'))
        .alias('corresponding_attribute_label')
    ).drop('tmp_attribute_label')

    pl_data = map_attribute_labels(pl_data, pl_attribute_map, 'corresponding_attribute_label', 'attribute_label')

    if not group_by_column:
        pl_data = pl_data.with_row_index("record_id").with_columns(
            pl.col('record_id').cast(pl.Utf8)
        )

    columns_to_split = list(set(pl_data.columns) & {'commodity', 'deposit_type_candidate', 'aliases'})

    pl_data = pl_data.with_columns(
        pl.col('commodity')
        .str.replace('REE', 'Lanthanum, Praseodymium, Samarium, Gadolinium, Thulium, Holmium, Cerium, Europium, Ytterbium, Erbium, Yttrium, Neodymium, Terbium, Dysprosium, Lutetium')
        .str.replace('PGE', 'Platinum, Palladium, Rhodium, Ruthenium, Iridium, Osmium')
    )

    pl_data = split_str_column(pl_data, columns_to_split)

    # Get EPSG code or CRS
    pl_data = pl_data.with_columns(
        pl.col('crs').map_elements(lambda x: get_epsg(x), return_dtype=pl.Utf8)
    )

    pl_data = normalize_dataframe(pl_data)

    pl_data = pl_data.with_columns(
        document = pl.struct(pl.col('uri'))
    ).with_columns(
        pl.col('country').map_elements(lambda x: [x]),
        pl.col('state_or_province').map_elements(lambda x: [x]),
        reference = pl.struct(pl.col('document')),
    )
    pl_data = pl_data.explode('commodity')

    # print(pl_data.columns)

    # with open('/users/2/pyo00005/HOME/CriticalMAAS/mid_point_data.pkl', 'wb') as handle:
    #     pickle.dump(pl_data, handle, protocol=pickle.HIGHEST_PROTOCOL)


    pl_data = pl_data.with_columns(
        mineral_inventory = pl.struct(pl.col(['commodity', 'reference'])).map_elements(lambda x: data_to_none(input_object=x, col_decision='commodity', col_affected='reference', col_sub='observed_name'))
    ).drop(['uri', 'document', 'commodity'])

    pl_data = pl_data.group_by('record_id').agg(
        [pl.all()]
    ).with_columns(
        pl.exclude(['record_id', 'mineral_inventory']).list.get(0)
    )

    as_json(pl_data, path_output_dir, path_filename)
    logging.info(f'Processed data stored in {path_output_dir} as {path_filename}.json - Elapsed Time: {time.time() - start_time}\n')

def main():
    parser = argparse.ArgumentParser(description='Processing data to suggested mineral site schema')

    parser.add_argument('--raw_data', required=True,
                        help='Directory or file where the raw mineral site databases are located')

    parser.add_argument('--attribute_map',
                        help='CSV file with label mapping information (see sample_mapfile.csv for reference)')

    parser.add_argument('--schema_output_directory', default='./outputs/',
                        help='Directory where the processed raw mineral site database will be stored')

    parser.add_argument('--schema_output_filename', required=True,
                        help='Filename for the processed raw mineral site database')

    args = parser.parse_args()
    process_rawdata(args)

if __name__ == '__main__':
    main()
