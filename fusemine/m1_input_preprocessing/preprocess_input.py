import os
from time import perf_counter

from fusemine.params import *
from fusemine.m1_input_preprocessing.preprocess_dataframe import process_dataframe
from fusemine.m1_input_preprocessing.preprocess_dictionary import process_dictionary
from fusemine.m1_input_preprocessing.preprocess_schema import process_schema

def preprocessing(list_code, mss_code):
    for i in list_code:
        # try:
        #     process_dictionary(i)
        # except:
        #     pass
        process_dictionary(i)
        process_dataframe(i)

    if mss_code == 'mss':
        process_schema(mss_code)
    