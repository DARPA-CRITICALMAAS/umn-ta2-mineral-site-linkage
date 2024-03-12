import pickle

file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_dictionary.pkl'

with open(file_name, 'rb') as handle:
    dictionary = pickle.load(handle)

# dictionary['name'] = ['Ftr_Name', 'Other_Name', 'Site', 'Site_Name', 'site', 'site_name', 'names']
dictionary['latitude'] = ['Latitude', 'latitude', 'Lat_WGS84', 'Approx_Lat']
dictionary['longitude'] = ['Longitude', 'longitude', 'Lon_WGS84', 'Approx_Lon']
# dictionary['commodities'] = ['Commodities_main', 'Commodities_other', 'Commodity', 'commod1', 'commod2', 'commod3']

with open(file_name, 'wb') as handle:
    pickle.dump(dictionary, handle, protocol=pickle.HIGHEST_PROTOCOL)

print(dictionary)