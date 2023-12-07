#!/usr/bin/python

import os
import shutil

from minelink.params import *

def remove_dir(path_dir=PATH_TMP_DIR, additional=''):
    """
    :input: path_dir (str) = path to directory that needs to be removed
    :       default: PATH_TMP_DIR
    :input: additional (str) = 
    :       default: ''
    """
    
    path_dir = os.path.join(path_dir, additional)
    
    try:
        shutil.rmtree(path_dir, ignore_errors=False)
    except:
        pass