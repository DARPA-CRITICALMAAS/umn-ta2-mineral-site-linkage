# UMN TA2 Mineral Site Linkage
This repository is for the reconciliation of geospatial datasets (e.g., mineral site databases - MRDS, USMIN)

## Requirements
Install the required package libraries by running the following code in the command line:
```
pip install -r requirements.txt
```

## How to run
Run the python file by entering the following code in the command line:
```
python -m minelink -d path/to/data/directory [-l if only using geolocation for linking]
```
The linked JSON output would be located under `./outputs`.

### Testing
```
python -m minelink.tester -p path/to/ground_truth_file -c column_with_linking
```

## Directory Layout
```
./minelink
|-- __init__.py
|-- __main__.py
|
|-- m0_save_and_load
|   |-- create_tmp_dir.py               # 
|   |-- load_data.py                    # 
|   |-- save_ckpt_as_pickle.py          # 
|
|-- m1_preprocessing
|   |-- column_mapping.py               # Reads the data dictionary and find columns relevant to lat/long/crs
|   |-- converting_to_geodataframe.py   #
|   |-- dataframe_preprocessing.py      #
|
|-- m2_location_based_linking
|   |-- link_with_loc.py                # Completes linking based on geolocation information
|
|-- m3_text_based_linking
|   |-- link_with_all.py                # Completes linking with geolocation and textual information
|   |-- create_documents.py             # Create the psuedo document that will be used for linking
|
|-- m4_intralinking
|   |-- intra_linking.py                # Completes linking within databases
|
|-- m5_interlinking
|   |-- create_representation.py        # Creates a representation of intra-linked clusters
|   |-- inter_linking.py                # Completes linking across databases
|
|-- m6_determine_linking_method
|   |-- determine_link_mode.py          # 
|
|-- m7_postprocessing
|   |-- dataframe_postprocessing.py     # Formats grouped database into form required for JSON output
|   |-- determine_location.py           # Determines the most confident location
|
|-- m8_save_output
|   |-- save_as_json.py                 # Saves output as a JSON file (in form of proposed schema)
|   |-- save_as_geojson.py              # Saves output as a GeoJSON file
|   |-- remove_tmp_dir.py               # Removes temporary directory that saved intermediate check points
|
|-- m9_testing
|   |-- evaluation_metrics.py           # 
|
```

## Updates
### In Progress
- [ ] Check time of column identification task (prev method v. current method)

### Completed (For: December 20th)
- [x] Create data dictionary refering method
- [x] Populate data dictionary processing
- [x] Update same as structure
- [x] Update postprocessing to reflect same as structure
- [x] Create a function that will determine the CRS value from the description of latitude
- [x] Bring back the column identification function
- [x] Finish function for developing information dictionary
- [x] When saving to geojson, also save the source of the file
- [x] Recheck dictionary for Alaska dataset
- [x] Populate text based linking method (create python file for each method)

### To-Do
- [ ] Create method to create short description when there is no column representing short description
- [ ] Finish m7.determine_location to select most confident location (not just the first)
- [ ] Develop a clustering algorithm that will allow inter-linking
- [ ] Develop method to test accuracy of each module (column_mapping, linking procedure)
- [ ] Make logging available for each module (move timing and accuracy to log)
- [ ] (Extended) Change to polars / geopolars

### Archive