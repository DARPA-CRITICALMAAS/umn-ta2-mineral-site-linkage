# from utils.load_save import *
# from utils.intra import *
# from utils.inter import *
# from intra_test import *

# df_site, dict_loc, dict_sameas, dict_geo = intra_group('MRDS_Zinc', 'Taylor', 'Zinc')

import pandas as pd

path = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/10.5066_P96MMRFD.csv'
df = pd.read_csv(path)

df = convert_to_gdb(df, col_longitude[0], col_latitude[0], crs='NAD27')