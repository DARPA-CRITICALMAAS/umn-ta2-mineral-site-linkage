import os
import pickle

def save_as_geojson(data, path_dir, file_name):        
    data = data[['source_name', 'site_name', 'geometry', 'GroupID']]

    data.to_file(os.path.join(path_dir, file_name+'.geojson'), driver='GeoJSON')