#!/usr/bin/python

import os
import shutil

from minelink.params import *

def close_ckpt_dir(list_path=[PATH_TMP_DIR]):
    path_dir = ''

    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    try:
        shutil.rmtree(path_dir, ignore_errors=False)
    except:
        pass