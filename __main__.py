from argparse import ArgumentParser
import logging

from minelink.m0_load_input.load_data import load_dir, load_kg
from minelink.m1_input_preprocessing.preprocess_input import preprocessing
from minelink.m1_input_preprocessing.preprocess_schema import process_schema

from minelink.m2_intralinking.intralink import intralink
from minelink.m2_intralinking.create_intra_representation import create_intra_rep
from minelink.m3_interlinking.interlink import interlink

from minelink.m4_postprocessing.postprocess_dataframe import postprocessing, postprocess_toGeoJSON
from minelink.m5_save_output.save_output import *

def main(args):
    # logging.basicConfig(filename='minelink_run.log', format='%(levelname)s:%(message)s', level=logging.INFO)
    # list_code = load_dir(path_dir=args.data_dir, bool_dict=True)
    # mss_code = load_kg()
    # process_schema(mss_code)

    # preprocessing(list_code, mss_code)
    
    # list_code = []
    # list_code.append(mss_code)
    # intralink(list_code, args.use_location_base)

    list_code = ['ah', 'ai', 'mss']

    print("interlink_start")
    interlink(list_code, args.use_location_base)
    print("interlink done")

    print("postprocess start")
    df_linked = postprocessing(bool_interlink=True)
    save_output_json(df_linked, 'output')
    print("postprocess done")

    list_code = ['ah', 'ai', 'mss']

    gdf_linked = postprocess_toGeoJSON(list_code, bool_interlink=True)
    save_output_geojson(gdf_linked, 'output', ['./'])

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())
