# myapp.py
import logging
# logger = logging.getLogger(__name__)

def main():
    logging.basicConfig(filename='./myapp.log', format='%(asctime)s: %(message)s', level=logging.INFO)
    logging.info('Started')
    print('hello')
    logging.info('Finished')
    

if __name__ == '__main__':
    main()

# import regex as re
# import polars as pl
# import pandas as pd
# import pickle

# # archive = {
# #     'record_id': ['Site_ID', 'Site_ID *', 'dep_id', 'rec_id']
# # }

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_archive.pkl', 'rb') as handle:
#     sample = pickle.load(handle)

# sample['record_id'].append('one')

# print(sample)

# from utils.compare_text import *
# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/https:pubs.usgs.govof20081155/pubs.usgs.gov/of/2008/1155/dictionary.csv'
# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/usmin/USGS_Gallium_US_CSV/Data_Dictionary.csv'
# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/MRDS/dictionary.csv'

# data_dictionary = pl.read_csv(file_name)

# print(data_dictionary)

# from utils.load_kg_data import *


# focus_commodity = 'copper'
# pl_data = load_minmod_kg(focus_commodity).drop_nulls(subset=['location', 'crs'])
# pl_data.write_csv('./copper.csv')

# focus_commodity = 'tungsten'
# pl_data = load_minmod_kg(focus_commodity).drop_nulls(subset=['location', 'crs'])
# pl_data.write_csv('./tungsten.csv')

# pl_MRDS = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/MRDS_commodity_code.csv').rename(
#     {
#         'Commodity name': 'CommodityinMRDS',
#         'Code': 'MRDS_Code'
#     }
# )

# minmod_commodities = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/entities/commodities/minmod_commodities.csv')
# pl_total = pl.concat(
#     [minmod_commodities, pl_MRDS],
#     how='align'
# )

# pl_nonodes = pl_total.filter(
#     pl.col('minmod_id').is_null()
# )

# list_values = list(range(637, 637+pl_nonodes.shape[0]))

# pl_nonodes = pl_nonodes.drop('minmod_id').with_columns(
#     minmod_id = pl.lit('Q') + pl.Series(list_values).cast(pl.Utf8)
# )

# pl_total = pl_total.drop_nulls(subset=['minmod_id'])

# pl_all = pl.concat(
#     [pl_total, pl_nonodes],
#     how='diagonal'
# ).sort(
#     'minmod_id'
# )

# pl_all.fill_null('').write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/commodities_map.csv')
# pl_all.drop(
#     'MRDS_Code'
# ).write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/entities/commodities/minmod_commodities.csv')

# print(pl_all)

# .replace(r"(?i)property|mine|mines|prospect|prospects|properties|claim|claims|occurrence|occurrences|deposit|deposits", "")

# site_suffix = 'property|mine|mines|prospect|prospects|properties|claim|claims|occurrence|occurrences|deposit|deposits'

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/site_suffix.pkl', 'rb') as handle:
#     regex_pattern = pickle.load(handle)

# print(regex_pattern)

# sample_string = 'hello mine'

# another_string = re.sub(regex_pattern, '', sample_string)

# print(another_string)

# import torch
# import torch.nn as nn

# from utils.load_kg_data import *
# from utils.create_textinfo_representation import *

# print(load_minmod_kg('cobalt'))

# device = 'cuda' if torch.cuda.is_available() else 'cpu'

# example = 'hello mine'
# example2 = 'hello'

# example_embedding = text_embedding(example)
# example_embedding2 = text_embedding(example2)


# # logging.info(f'Fusemine running on {device}')
# print(type(example_embedding))
# print(type(example_embedding2))


# cos = nn.CosineSimilarity(dim=0, eps=1e-6)
# torch_version1 = torch.from_numpy(example_embedding).to(device)
# torch_version2 = torch.from_numpy(example_embedding2).to(device)

# # emb1 = torch.Tensor(embedding1).to(device)
# # emb2 = torch.Tensor(embedding2).to(device)
# # emb3 = torch.Tensor(embedding3).to(device)
# # emb4 = torch.Tensor(embedding4).to(device)

# cosine_similarity1 = cos(torch_version1, torch_version2)
# similarity_score = cosine_similarity1.item()

# cosine_similarity2 = cos(emb3, emb4)