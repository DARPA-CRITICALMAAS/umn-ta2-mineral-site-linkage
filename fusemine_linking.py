import argparse
from fusemine import FuseMine

def main(commodity:str=None,
         country:str=None, state:str=None,
         start_fresh:bool=False,
         input_data:str=None,
         output_directory:str=None) -> None:

    fusemine = FuseMine(commodity=commodity,
                        country=country,
                        state=state,
                        dir_output=output_directory,
                        start_fresh=start_fresh)
    
    # Load data from minmod or user input datafile (if exists)
    if input_data:
        fusemine.load_data(, 
                           input_data=input_data,method='data')
    else:
        fusemine.load_data()

    # Prepare data for linking purpose (unify crs, serialize data etc)
    fusemine.prepare_data()

    # # Load data file, load map file, check output directory, check entities directory
    # procmine.prepare_data_paths()

    # # Process database
    # fusemine.link()
        
    # Save output
    # fusemine.save_output()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TA2 FuseMine-Mineral Site Linking')

    parser.add_argument('--commodity',
                        help="Specific commodity to use FuseMine (Special Options: 'all', 'critical')")

    parser.add_argument('--country',
                        help="Specific region to use FuseMine")
    
    parser.add_argument('--state',
                        help='Specific state to use FuseMine')
    
    parser.add_argument('--ignore_previous_sameas', '--ignore', default=False, type=bool,
                        help="If ignore previous sameas links set this to True")
    
    parser.add_argument('--input_data',
                        help="If run FuseMine with data on KG and local data. Local data will not be pushed to minmod. Sameas links will not be pushed to minmod.")
    
    parser.add_argument('--output_directory',
                        help='Directory for processed mineral site database')
    
    args = parser.parse_args()

    main(commodity=args.commodity, 
         country=args.country, state=args.state
         start_fresh=args.ignore_previous_sameas,
         input_data=args.input_data,
         output_directory=args.output_directory)