# from math import radians

# import polars as pl
# import geopandas as gpd
# from shapely import wkt

# from utils.convert_dataframe import *
# from sklearn.metrics.pairwise import haversine_distances

# def calculate_haversine(point_1:str, point_2:str):
#     point_1 = wkt.loads(point_1)
#     point_2 = wkt.loads(point_2)
#     if point_1.geom_type == 'MultiPoint':
#         point_1 = point_1.centroid

#     if point_2.geom_type == 'MultiPoint':
#         point_2 = point_2.centroid

#     location_1 = [point_1.x, point_1.y]
#     location_2 = [point_2.x, point_2.y]

#     location_1_rad = [radians(_) for _ in location_1]
#     location_2_rad = [radians(_) for _ in location_2]

#     value = haversine_distances([location_1_rad, location_2_rad]) * 6371000/1000

#     return value[0][1]

# pl_data = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/tungsten_grouped.csv')

# gpd_output = to_geopandas(pl_data, 'pl', geometry_column='location')

# gpd_series = gpd_output.dissolve(
#     'GroupID'
# ).convex_hull.centroid

# gpd_output = gpd.GeoDataFrame(geometry=gpd_series)
# pd_output = pd.DataFrame(gpd_output)
# pd_output = pd_output.reset_index()
# pd_output['geometry'] = pd_output['geometry'].apply(lambda x: wkt.dumps(x))

# groupID = pd_output['GroupID'].tolist()
# centroid = pd_output['geometry'].tolist()

# dict_mapping = dict(zip(groupID, centroid))

# pl_data = pl_data.with_columns(
#     centroid = pl.col('GroupID').replace(dict_mapping)
# ).with_columns(
#     distance = pl.struct(pl.all()).map_elements(lambda x: calculate_haversine(x['centroid'], x['location']))
# )

# pl_data = pl_data.filter(
#     pl.col('distance') > 0
# ).select(
#     pl.col(['GroupID', 'country', 'state_or_province', 'location', 'site_name', 'distance'])
# ).sort(by='distance')

# pl_data.write_csv('./distance_check.csv')

# pl_data = pl.read_excel('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/base_metal_deposit_compilation_hoggardetal2020.xls')

# print(pl_data)

# import strsim
# import regex as re
# import statistics

# def does_ordinal_match(s1: str, s2: str, sim: float, threshold: float) -> float:
#     """Test for strings containing ordinal categorical values such as Su-30 vs Su-25, 29th Awards vs 30th Awards.

#     Args:
#         s1: Cell Label
#         s2: Entity Label
#     """
#     if sim < threshold:
#         return 0.4

#     digit_tokens_1 = re.findall(r"\d+", s1)
#     digit_tokens_2 = re.findall(r"\d+", s2)

#     if digit_tokens_1 == digit_tokens_2:
#         return 1.0

#     if len(digit_tokens_1) == 0 or len(digit_tokens_2) == 0:
#         return 0.4

#     return 0.0

# chartok = strsim.CharacterTokenizer()
# charseqtok = strsim.WhitespaceCharSeqTokenizer()

# text = 'fluorspar'
# entity_label = 'fluorine'

# text_t1 = chartok.tokenize(text)
# entity_label_t1 = chartok.tokenize(entity_label)

# text_t2 = charseqtok.tokenize(text)
# entity_label_t2 = charseqtok.tokenize(entity_label)

# text_t3 = charseqtok.unique_tokenize(text)
# entity_label_t3 = charseqtok.unique_tokenize(entity_label)

# out2 = [
#     strsim.levenshtein_similarity(text_t1, entity_label_t1),
#     strsim.jaro_winkler_similarity(text_t1, entity_label_t1),
#     strsim.monge_elkan_similarity(text_t2, entity_label_t2),
#     (
#         sym_monge_score := strsim.symmetric_monge_elkan_similarity(
#             text_t2, entity_label_t2
#         )
#     ),
#     (hyjac_score := strsim.hybrid_jaccard_similarity(text_t3, entity_label_t3)),
#     does_ordinal_match(text, entity_label, sym_monge_score, 0.7),
#     does_ordinal_match(text, entity_label, hyjac_score, 0.7),
# ]

# print(statistics.mean(out2))

import polars as pl
import pickle
from itertools import combinations

pl_gt = pl.read_excel('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/ground_truth_data/nickel_gt.xlsx').select(
    pl.col(['ms_uri', 'group'])
)

uri_list = pl_gt['ms_uri'].to_list()

dict_gt = dict(zip(uri_list, pl_gt['group'].to_list()))

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/ground_truth_data/nickel_gt.pkl', 'wb') as handle:
#     pickle.dump(pl_data, handle, protocol=pickle.HIGHEST_PROTOCOL)

pl_pred = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/nickled_linked.csv').with_columns(
    pl.col('ms_uri').str.split(',')
).explode('ms_uri').filter(
    pl.col('ms_uri').is_in(uri_list)
)

dict_pred = dict(zip(pl_pred['ms_uri'].to_list(), pl_pred['GroupID'].to_list()))

combination_list = combinations(uri_list, 2)

list_gt = []
list_pred = []

correct_count = 0
total_count = 0

for one, two in combination_list:
    if (dict_gt[one] == dict_gt[two]) and (dict_pred[one] == dict_pred[two]):
        correct_count += 1
    if (dict_gt[one] != dict_gt[two]) and (dict_pred[one] != dict_pred[two]):
        correct_count += 1
    total_count += 1

pl_gt = pl_gt.group_by('group').agg([pl.all()]).with_columns(
    pl.col('ms_uri').list.sort().list.join(" ")
)['ms_uri'].to_list()
pl_pred = pl_pred.group_by('GroupID').agg([pl.all()]).with_columns(
    pl.col('ms_uri').list.sort().list.join(" ")
)['ms_uri'].to_list()

overlap = set(pl_gt) & set(pl_pred)

print(len(list(overlap)) / len(pl_gt))


for i in list(set(pl_gt) - overlap):
    print(i)

print(correct_count / total_count)

pl_sameas = pl.read_csv('./output/nickel_sameas.csv').filter(
    pl.col('ms_uri_1').is_in(uri_list)
).filter(
    pl.col('ms_uri_2').is_in(uri_list)
).unique(subset=['ms_uri_2'], keep='first').group_by('ms_uri_1').agg([pl.all()]).with_columns(
    pl.col('ms_uri_2').list.sort().list.join(" ")
)['ms_uri_2'].to_list()

print((len(list((set(pl_sameas)) & set(pl_gt))) + 2) / len(pl_gt))

print(set(pl_gt) - set(pl_sameas))

print(len(pl_gt))
# print(pl_sameas)

# pl_sameas = pl_sameas.group_by('ms_uri_1').agg([pl.all()]).with_columns(
#     pl.col('ms_uri_2').list.sort().list.join(" ")
# ).unique(subset=['ms_uri_2'], keep='first')['ms_uri_2'].to_list()

# overlap = set(pl_gt) & set(pl_sameas)
# print(len(list(overlap)) / len(pl_sameas))