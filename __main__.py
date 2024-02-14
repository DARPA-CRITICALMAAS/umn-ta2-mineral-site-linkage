from argparse import ArgumentParser
import logging

from m0_load_input.load_data import load_dir, load_mss, load_kg
from m1_input_preprocessing.preprocess_input import preprocessing
from m1_input_preprocessing.preprocess_schema import process_schema

from m2_intralinking.intralink import intralink
from m2_intralinking.create_intra_representation import create_intra_rep
from m3_interlinking.interlink import interlink

from m4_postprocessing.postprocess_dataframe import postprocessing, postprocess_toGeoJSON
from m5_save_output.save_output import *

def main(args):
    # logging.basicConfig(filename='minelink_run.log', format='%(levelname)s:%(message)s', level=logging.INFO)
    list_code = load_dir(path_dir=args.data_dir, bool_dict=True)
    mss_code = load_kg()

    preprocessing(list_code, mss_code)
    
    list_code.append(mss_code)
    intralink(list_code, args.use_location_base)

    interlink_code_copy = list_code
    interlink(interlink_code_copy, args.use_location_base)

    df_linked = postprocessing(bool_interlink=True)
    save_output_json(df_linked, 'output')

    gdf_linked = postprocess_toGeoJSON(list_code, bool_interlink=True)
    save_output_geojson(gdf_linked, 'output', ['./'])

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())
