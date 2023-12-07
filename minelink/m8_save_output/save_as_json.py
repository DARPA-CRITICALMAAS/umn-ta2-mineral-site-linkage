import os
from json import loads, dumps

def save_as_json(data, path_dir, file_name):
    json_df = data.to_json(orient='index', default_handler=str)    # default handler set to prevent iteration overflow

    json_data = loads(json_df)
    obj_data = dumps(json_data, indent=4)

    with open(os.path.join(path_dir, file_name+'.json'), 'w') as handle:
        handle.write(obj_data)