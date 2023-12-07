import os
import pickle5 as pickle

# def dump_file(data, path_dir, file_name, save_format):
#     if save_format.upper() == 'PKL' or save_format.upper() == 'PICKLE':
#         with open(os.path.join(path_dir, file_name+'.pkl'), 'wb') as handle:
#             pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

#     elif save_format.upper() == 'CSV':
#         data.to_csv(os.path.join(path_dir, file_name + '.csv'))

def save_ckpt(data, path_dir, file_name):
    with open(os.path.join(path_dir, file_name+'.pkl'), 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)