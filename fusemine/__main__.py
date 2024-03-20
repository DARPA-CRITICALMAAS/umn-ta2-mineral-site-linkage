from argparse import ArgumentParser
import logging

from fusemine.m0_load_input.load_data import load_dir, load_kg
from fusemine.m1_input_preprocessing.preprocess_input import preprocessing
from fusemine.m1_input_preprocessing.preprocess_schema import process_schema

from fusemine.m2_intralinking.intralink import intralink
from fusemine.m2_intralinking.create_intra_representation import create_intra_rep
from fusemine.m3_interlinking.interlink import interlink

from fusemine.m4_postprocessing.postprocess_dataframe import postprocessing
from fusemine.m5_save_output.save_output import *

def main(args):
    list_code = load_dir(path_dir=args.data_dir, bool_dict=True)
    mss_code = load_kg()

    process_schema(mss_code)
    preprocessing(list_code, mss_code)
    
    intralink(list_code, args.use_location_base)
    interlink(list_code, args.use_location_base)

    df_linked = postprocessing(bool_interlink=True)

    output_filename = input('Output file name')
    save_output_csv(df_linked, output_filename)

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())
