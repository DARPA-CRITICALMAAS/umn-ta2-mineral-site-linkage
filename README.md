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
|-- m0_load_input
|
|-- m1_input_preprocessing
|
|-- m2_intralinking
|
|-- m3_interlinking
|
|-- m4_postprocessing
|
|-- m5_save_output
|
|-- m6_testing
```


### Archive
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
|   |-- converting_to_geodataframe.py   # Converts different databases into geodataframe with same crs
|   |-- dataframe_preprocessing.py      # Preprocesses the input data into form for the program to run
|
|-- m2_location_based_linking
|   |-- link_with_loc.py                # Completes linking based on geolocation information
|
|-- m3_text_based_linking
|   |-- create_documents.py             # Create the psuedo document that will be used for linking
|   |-- link_with_all.py                # Completes linking with geolocation and textual information
|
|-- m4_intralinking
|   |-- intra_linking.py                # Completes linking within databases
|   |-- create_representation.py        # Creates a representation of intra-linked clusters
|
|-- m5_interlinking
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