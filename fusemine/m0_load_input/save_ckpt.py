import os
import pickle5 as pickle

from fusemine.params import *

def open_ckpt_dir(list_path=[PATH_TMP_DIR]):
    path_dir = ''

    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

def save_ckpt(data, list_path, file_name):
    path_dir = ''

    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    with open(os.path.join(path_dir, file_name+'.pkl'), 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)