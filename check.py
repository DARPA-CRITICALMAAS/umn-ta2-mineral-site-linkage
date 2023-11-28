# import pandas as pd

# df = pd.read_excel('/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/Leach2005.xls')

# print(df)

# from tabulate import tabulate

# data = [["Name","Age"],["Alice",24],["Bob",19]]

# print(tabulate(data, headers='firstrow', tablefmt="presto"))

# import os
# import pickle5 as pickle

# target_dictionary = {
#     "name":"name of site or name of deposit",
#     "latitude":"latitude",
#     "longitude":"longitude",
#     "crs":"coordinate reference system",
# }

# with open(os.path.join('./', 'target'+'.pkl'), 'wb') as handle:
#     pickle.dump(target_dictionary, handle, protocol=pickle.HIGHEST_PROTOCOL)

import regex as re

sample = ['dep_id', 'id_dep', 'dep_id_rec', 'DepId', 'idDep', 'identity']

for i in sample:
    splitted_col_name = re.split('[^A-Za-z]', i.lower())

    if 'id' in splitted_col_name:
        print("True")
    elif re.search('([^A-Za-z]+Id|Id[^A-Za-z]+)', i):
        print(i)
    else:
        print(splitted_col_name, i)

    # print(re.search('[^A-Za-z]?id[^A-Za-z]?', i), i)