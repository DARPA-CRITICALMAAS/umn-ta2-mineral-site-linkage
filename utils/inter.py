# import transformers
import os

from utils.format import *
from utils.params import *
from utils.load_save import *
from utils.dictionary import *
from utils.document import *
from utils.grouping import *

from sklearn.cluster import HDBSCAN

import warnings
warnings.filterwarnings("ignore")

def select_geometry(dataframe):
    print(dataframe['geometry'])

def inter_group(filename1, filename2):
    df1 = pickle_load('./' + filename1, 'df_rep')
    df2 = pickle_load('./' + filename2, 'df_rep')

    col_common = list(set(df1.columns) & set(df2.columns))

    df1 = df1[col_common]
    df2 = df2[col_common]

    df_total = pd.concat([df1, df2], axis=0)
    df_total.reset_index(drop=True, inplace=True)

    df_total['geometry'] = df_total['geometry'].apply(lambda x: x[0])

    geo_dataframe = gpd.GeoDataFrame(df_total)
    geo_dataframe = geo_dataframe.set_geometry('geometry')
    geo_dataframe.crs = 'NAD83'

    df = location_group(geo_dataframe)
    gpd_grouped = df.groupby(['GroupID']).agg(lambda x: list(x))

    gpd_grouped.to_csv('tmp.csv')

    print(gpd_grouped)

    # print(gpd_grouped)

    # return 0