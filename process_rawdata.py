from utils.load_files import *
from utils.save_files import *
from utils.dataframe_operations import *

def main(path_rawdata:str, path_attribute_map:str, path_output_dir:str, path_filename:str):
    pl_attribute_map = initiate_load(path_attribute_map)
    pl_attribute_map = pl_attribute_map.drop_nulls(subset=['corresponding_attribute_label'])

    group_by_column = pl_attribute_map.filter(
        pl.col('attribute_label') == 'record_id'
    ).item(0, 'corresponding_attribute_label')
    dict_attribute_map = as_dictionary(pl_attribute_map, 'corresponding_attribute_label', 'attribute_label')

    # For the case where the input is a directory
    file_names = pl_attribute_map.unique(
        'file_name'
    )['file_name'].to_list()

    file_names = list(filter(None, file_names))

    if len(file_names) > 1:
        pl_data = load_directory(path_rawdata, file_names, group_by_col=group_by_column)
    else:
        pl_data = initiate_load(path_rawdata, False)

    pl_data = map_attribute_labels(pl_data, dict_attribute_map)

    columns_to_map = list(set(pl_data.columns) & {'commodity', 'deposit_type'})
    pl_data = split_str_column(pl_data, columns_to_map)

    prefix = 'https://minmod.isi.edu/resource/' # CONFIG FILE

    try:
        commodities_map = initiate_load('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/commodities_map.csv') # CONFIG FILE
        pl_data = map_node_values(pl_rawdata=pl_data, pl_value_map=commodities_map,
                            column_to_map='commodity', map_column_as='commodity', value_map_from=['CommodityinMRDS', 'CommodityinGeoKb', 'CodeinMRDS'], value_map_to='minmod_id', bool_optional=False,
                            prefix=prefix, other_column_to_include='uri', store_original_value='observed_commodity').rename({'tmp':'mineral_inventory'})
    except:
        pass

    try:
        deposit_type_map = initiate_load('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/deposit_type.csv') # CONFIG FILE
        pl_data = map_node_values(pl_rawdata=pl_data, pl_value_map=deposit_type_map,
                            column_to_map='deposit_type', map_column_as='normalized_uri', value_map_from=['Deposit type'], value_map_to='Minmod ID', bool_optional=True,
                            prefix=prefix, store_original_value='observed_name').rename({'tmp':'deposit_type_candidate'})
    except:
        pass

    try:
        state_map = initiate_load('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/states_map.csv')
        pl_data = map_values(pl_rawdata=pl_data, pl_value_map=state_map,
                             column_to_map='state_or_province', map_column_as="state_or_province", value_map_from='State_Abbreviation', value_map_to='State')
    except:
        pass

    # print(pl_data)
    # individual_length = int((pl_data.shape[0])/12)

    # start_point = 0
    # for i in range(individual_length):
    #     pl_tmp = pl_data[start_point:start_point+individual_length]
    #     print(pl_tmp)
    #     as_json(pl_tmp, path_output_dir, f'{path_filename}_{str(i+1)}')

    #     start_point += individual_length

    as_json(pl_data, path_output_dir, path_filename)

# list_commodity = ['Gallium', 'Germanium', 'Graphite', 'Indium', 'Niobium', 'Rhenium', 'Sn', 'Tantalum', 'Tungsten']
list_commodity = ['Cobalt', 'Lithium', 'REE', 'Tellurium']

for commodity in list_commodity:
    main(f'/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/usmin/USGS_{commodity}_US_CSV', '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/USMIN_mapfile.csv', '/home/yaoyi/pyo00005/CriticalMAAS/src/outputs', f'USMIN_{commodity}')