import logging
import configparser

from utils.load_files import *
from utils.save_files import *
from utils.dataframe_operations import *

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']
map_params = config['mapping.prefix']

def process_rawdata(path_rawdata:str, path_attribute_map:str, path_output_dir:str, path_filename:str):
    start_time = time.time()
    try:
        pl_attribute_map = initiate_load(path_attribute_map)
        pl_attribute_map = pl_attribute_map.drop_nulls(subset=['corresponding_attribute_label'])
    except:
        logging.info('Cannot locate attribute map. Using attribute label identification to process data. This may lead to incorrect results.')
        pass

    group_by_column = pl_attribute_map.filter(
        pl.col('attribute_label') == 'record_id'
    ).item(0, 'corresponding_attribute_label')

    list_record_id_archive = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'attribute_archive.pkl'))
    list_record_id_archive['record_id'].append(group_by_column)
    as_pkl(list_record_id_archive, os.path.join(path_params['PATH_RSRC_DIR'], 'attribute_archive.pkl'))

    # For the case where the input is a directory
    file_names = pl_attribute_map.unique(
        'file_name'
    )['file_name'].to_list()

    file_names = list(filter(None, file_names))

    if len(file_names) > 1:
        pl_data = load_directory(path_rawdata, file_names, group_by_col=group_by_column)
    else:
        pl_data = initiate_load(path_rawdata, False)

    pl_data = map_attribute_labels(pl_data, pl_attribute_map, 'corresponding_attribute_label', 'attribute_label')

    columns_to_map = list(set(pl_data.columns) & {'commodity', 'deposit_type'})
    pl_data = split_str_column(pl_data, columns_to_map)

    prefix = map_params['MINMOD_PREFIX']

    try:
        commodities_map = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'commodities_map.csv'))
        pl_data = map_node_values(pl_rawdata=pl_data, pl_value_map=commodities_map,
                            column_to_map='commodity', map_column_as='commodity', value_map_from=['CommodityinMRDS', 'CommodityinGeoKb', 'CodeinMRDS'], value_map_to='minmod_id', bool_optional=False, bool_code=True,
                            prefix=prefix, other_column_to_include='uri', store_original_value='observed_commodity').rename({'tmp':'mineral_inventory'})
    except:
        pass

    try:
        deposit_type_map = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'deposit_type.csv'))
        pl_data = map_node_values(pl_rawdata=pl_data, pl_value_map=deposit_type_map,
                            column_to_map='deposit_type', map_column_as='normalized_uri', value_map_from=['Deposit type'], value_map_to='Minmod ID', bool_optional=True,
                            prefix=prefix, store_original_value='observed_name').rename({'tmp':'deposit_type_candidate'})
    except:
        pass

    try:
        state_map = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'states_map.csv'))
        pl_data = map_values(pl_rawdata=pl_data, pl_value_map=state_map,
                            column_to_map="state_or_province", value_map_from='State_Abbreviation', value_map_to='State')
    except:
        pass

    as_json(pl_data, path_output_dir, path_filename)
    logging.info(f'Processed data stored in {path_output_dir} as {path_filename}.json - Elapsed Time: {time.time() - start_time}\n')