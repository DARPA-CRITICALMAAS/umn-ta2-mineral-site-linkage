import os

from minelink.params import *
from minelink.m1_input_preprocessing.preprocess_dataframe import process_dataframe
from minelink.m1_input_preprocessing.preprocess_dictionary import process_dictionary

def preprocessing(list_code):
    for i in list_code:
        # try:
        #     process_dictionary(i)
        # except:
        #     pass
        
        process_dataframe(i)