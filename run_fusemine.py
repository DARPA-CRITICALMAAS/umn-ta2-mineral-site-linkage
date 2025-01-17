import pickle # TODO: delete later
import argparse
from fusemine import FuseMine

def main(commodity:str,
         country:str, state:str,
         start_fresh:bool,     # Later change to False for using the sameas version
         geo_method:str,
         text_method:str,
         input_data:str=None,
         output_directory:str=None,
         
         is_dev:bool=False) -> None:

    fusemine = FuseMine(commodity=commodity,
                        country=country,
                        state=state,
                        dir_output=output_directory,
                        location_method=geo_method, text_method=text_method,
                        start_fresh=start_fresh)

    # Load data (either from KG only or both)
    if input_data:
        fusemine.load_data(input_data=input_data, method='data')
    else:
        fusemine.load_data()

    # Prepare data for linking purpose (unify crs, serialize data etc)
    fusemine.prepare_data()

    # with open('./fusemine_model.pkl', 'wb') as handle:
    #     pickle.dump(fusemine, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # with open('./fusemine_model.pkl', 'rb') as handle:
    #     fusemine = pickle.load(handle)

    # Process database
    if start_fresh:
        fusemine.fresh_link()
    else:
        fusemine.default_link()
    
    # # Finalize links
    # fusemine.identify_links()

    # # Save output
    # fusemine.save_output()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TA2 FuseMine-Mineral Site Linking')

    parser.add_argument('--commodity',
                        help="Specific commodity to use FuseMine (Special Options: 'all', 'critical')")

    parser.add_argument('--country', default='all', type=str,
                        help="Specific region to use FuseMine")
    
    parser.add_argument('--state', default='all', type=str,
                        help='Specific state to use FuseMine')
    
    parser.add_argument('--ignore_previous_sameas', '--ignore', default=True, type=bool,
                        help="If ignore previous sameas links set this to True")
        
    parser.add_argument('--input_data',
                        help="If run FuseMine with data on KG and local data. Local data will not be pushed to minmod. Sameas links will not be pushed to minmod.")

    parser.add_argument('--output_directory', default='./output', type=str,
                        help='Directory for processed mineral site database')

    parser.add_argument('--train', 
                        help='Retrain the text-based linking FuseMine model with available sameas links')
    
    parser.add_argument('--geo_method', default='distance', type=str,
                        help='Method to use for linking based on location attributes on the records (Options: distance/area)')

    parser.add_argument('--text_method', default='cosine', type=str,
                        help='Method to use for linking based on textual attributes on the records (Options: classify/cosine)')
    
    parser.add_argument('--dev', action='store_true')
    
    args = parser.parse_args()

    main(commodity=args.commodity, 
         country=args.country, state=args.state,
         start_fresh=args.ignore_previous_sameas,
         geo_method=args.geo_method, text_method=args.text_method,
         input_data=args.input_data,
         output_directory=args.output_directory,
         is_dev=args.dev)