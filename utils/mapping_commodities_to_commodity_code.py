import os
import time
import logging
import configparser

import polars as pl

from utils.load_files import initiate_load
from utils.save_files import as_csv
from utils.dataframe_operations import *

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']

def map_commodities_to_code():
    # dict_mrds_map = initiate_load(
    #     input_filename=os.path.join(path_params['PATH_RSRC_DIR'],
    #                                 path_params['PATH_MRDS_COMMODITY_CODE_FILE']), 
    #     bool_asdict=True, 
    #     key_column='Commodity name', 
    #     value_column='Code')
    
    pl_mrds_codes = initiate_load(input_filename=os.path.join(path_params['PATH_RSRC_DIR'],
                                                              path_params['PATH_MRDS_COMMODITY_CODE_FILE']))
    pl_minmod_commods = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 
                                                    path_params['PATH_MINMOD_COMMODITY_FILE']))
    pl_minmod_commods = pl_minmod_commods.with_columns(
        CodeinMRDS = pl.col('CommodityinMRDS')
    ).filter(
        pl.col('CodeinMRDS').is_in(pl_mrds_codes['Commodity name'].to_list())
    )

    pl_minmod_commods = map_values(pl_minmod_commods, pl_mrds_codes,
                                   column_to_map='CodeinMRDS', value_map_from='Commodity name', value_map_to='Code')
    as_csv(pl_data=pl_minmod_commods, 
           output_directory=path_params['PATH_RSRC_DIR'], 
           output_file_name=path_params['PATH_COMMODITY_MAP_FILE'], 
           bool_sameas=False)