import pickle

with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/fusemine/src/dict_known.pkl', 'rb') as handle:
    dictionary = pickle.load(handle)

# dictionary['name'].append('name')

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/fusemine/src/dict_known.pkl', 'wb') as handle:
#     pickle.dump(dictionary, handle, protocol=pickle.HIGHEST_PROTOCOL)

print(dictionary)