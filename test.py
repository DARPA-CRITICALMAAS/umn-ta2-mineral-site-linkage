import argparse

# Temporary helpers
import pandas as pd
import polars as pl
#

from utils.load_kg_data import *
from utils.compare_geolocation import *
from utils.compare_text import *

from utils.convert_dataframe import *
from utils.combine_grouping_results import *

bool_singlestage = False
methods = ['distance', '', '', '']

# pl_data = load_minmod_kg('nickel').drop_nulls(subset=['location', 'crs])
pl_data = pl.read_csv('./archive.csv')

if bool_singlestage:
    print("will be completed")
    pl_data = compare_geolocation(pl_data, method=methods[0])
    
else:
    # ------ Data Separation ------ #
    list_pl_by_source = pl_data.partition_by('source_id')

    # --------- Intralink --------- #
    for pl_data in list_pl_by_source:
        source_id = pl_data.item(0, 'source_id')
        print(source_id)
        pl_data = compare_geolocation(pl_data, source_id, methods[0])
        
        if methods[1] == 'Y':
            print('text')

        # merge_grouping_results(pl_grouped)

    # --------- Interlink --------- #
    if methods[2] == 'Y':
        pl_data = compare_geolocation(pl_data, method=methods[0])
    
    if methods[3] == 'Y':
        print('text')

    # merge_grouping_results(pl_grouped)

