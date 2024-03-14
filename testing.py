# import pickle

# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_dictionary.pkl'

# with open(file_name, 'rb') as handle:
#     dictionary = pickle.load(handle)

# dictionary['name'] = ['Ftr_Name', 'Other_Name', 'Site', 'Site_Name', 'site', 'site_name', 'names']
# dictionary['latitude'] = ['Latitude', 'latitude', 'Lat_WGS84', 'Approx_Lat']
# dictionary['longitude'] = ['Longitude', 'longitude', 'Lon_WGS84', 'Approx_Lon']
# dictionary['observed_name'] = ['dep_type', 'Deposit_model']
# dictionary['commodities'] = ['Commodities_main', 'Commodities_other', 'Commodity', 'commod1', 'commod2', 'commod3']

# with open(file_name, 'wb') as handle:
#     pickle.dump(dictionary, handle, protocol=pickle.HIGHEST_PROTOCOL)

# print(dictionary)

# import pickle
# import polars as pl

# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/data/6_month_hackathon/Nickel/USGS_Cobalt_US_CSV/Dep_Model.csv'

# df_dep_model = pl.read_csv(file_name, encoding='utf8-lossy')

# original_file = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/data/raw/USMIN.csv'
# df = pl.read_csv(original_file, encoding='utf8-lossy')


# print(df.columns)

# print(df)

import pickle
import pandas as pd
import polars as pl

# dictionary = {}

# dictionary['name'] = 'name'
# dictionary['latitude'] = 'latitude'
# dictionary['longitude'] = 'longitude'
# dictionary['crs'] = 'crs'
# dictionary['country'] = 'country'
# dictionary['state_or_province'] = 'state'
# dictionary['deposit_type'] = 'dep_type'

# print(dictionary)

# df = pd.DataFrame.from_dict(dictionary, orient='index', columns=['Matching Attributes in Database'])
# df = df.reset_index()
# df = df.rename(columns = {'index': 'attribute'})

# df.to_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_map.csv')

df = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_map.csv', encoding = 'utf8-lossy')
list_attribute = df['attribute'].to_list()
list_mapped = df['Matching Attributes in Database'].to_list()

dictionary = dict(zip(list_attribute, list_mapped))

print(dictionary)

# df = pd.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/attribute_map.csv')
# print(df)

