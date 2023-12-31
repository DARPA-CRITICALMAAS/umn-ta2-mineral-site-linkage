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
|   |-- load_data.py                        # Load all mineral site databases that is present in the user-inputted directory
|   |-- save_ckpt.py                        # Saves intermediate results (e.g. intralinking outputs, processed dictionaries) in the 'temporary' folder
|
|-- m1_input_preprocessing
|   |-- identify_columns.py                 # Identifies necessary attributes (e.g. unique ID, name, lat/long) from each database
|   |-- preprocess_dataframe.py             # Extracts parts required for the linking procedure from each database
|   |-- preprocess_dictionary.py            # 
|   |-- preprocess_input.py                 # Initiates the preprocessing step
|
|-- m2_intralinking
|   |-- create_intra_representation.py      # Creates a location and text representation of the intragrouped results
|   |-- intralink.py                        # Initiates intralinking
|   |-- location_based.py                   # Completes linking based on geolocation
|   |-- text_based.py                       # Completes linking based on textual attributes
|
|-- m3_interlinking
|   |-- interlink.py                        # Initiates interlinking
|   |-- location_based.py                   # Completes linking based on geolocation
|   |-- text_based.py                       # Completes linking based on textual attributes
|
|-- m4_postprocessing
|   |-- postprocess_dataframe.py            # Formats the linked result into the format required by the schema
|
|-- m5_save_output
|   |-- remove_ckpt.py                      # Removes all checkpoints that were made during itermediate processes
|   |-- save_output.py                      # Saves the output as a compiled JSON file (schema form) and GroupID appended GEOJSON file
|
|-- m6_testing
```