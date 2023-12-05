import pickle5 as pickle


path = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/temporary/aa/site_name.pkl'

with open(path, 'rb') as handle:
    dataframe = pickle.load(handle)

print(dataframe)