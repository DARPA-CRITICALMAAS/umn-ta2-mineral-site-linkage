import regex as re
import polars as pl

from fusemine.params import *
from fusemine.m0_load_input.save_ckpt import save_ckpt
from fusemine.m0_load_input.load_data import load_file

def process_dictionary(alias_code):
    pl_dict = load_file(list_path=[PATH_TMP_DIR, alias_code], 
                        file_name='dictionary', 
                        file_extension='.pkl')

    dictionary_columns = pl_dict.columns

    label = ''
    short = ''
    long = ''

    for i in dictionary_columns:
        if re.search('short', i.lower()):
            short = i
        elif re.search('descri', i.lower()) or re.search('defin', i.lower()):
            long = i
        elif re.search('label', i.lower()) or re.search('name', i.lower()):
            label = i

    if short == '':
        short = label

    pl_dict = pl_dict.select(
        label = pl.col(label).str.strip_chars(),
        short = pl.col(short).str.strip_chars(),
        long = pl.col(long).str.strip_chars(),
    )

    # if short == '':
    #     # TODO: add a way to extract short description from the long description
    #     pass

    dict_short = dict(zip(pl_dict['label'], pl_dict['short']))
    dict_long = dict(zip(pl_dict['label'], pl_dict['long']))

    save_ckpt(data=dict_short, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='mini_dictionary')
    
    save_ckpt(data=dict_long, 
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='reg_dictionary') 