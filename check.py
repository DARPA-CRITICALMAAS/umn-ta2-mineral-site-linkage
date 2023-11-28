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

import pandas as pd

data = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/GeologyMineralOccurrences_USCanada_Australia.csv'

df = pd.read_csv(data)
df = df.groupby(['Dep_Name']).agg(lambda x: list(x))

print(df)

df.to_csv('tmp.csv')