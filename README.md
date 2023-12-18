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
|   |-- load_data.py
|   |-- save_ckpt.py
|
|-- m1_input_preprocessing
|   |-- identify_columns.py
|   |-- preprocess_dataframe.py
|   |-- preprocess_dictionary.py
|   |-- preprocess_input.py
|
|-- m2_intralinking
|   |-- create_intra_representation.py
|   |-- intralink.py
|   |-- location_based.py
|   |-- text_based.py
|
|-- m3_interlinking
|   |-- interlink.py
|   |-- location_based.py
|   |-- text_based.py
|
|-- m4_postprocessing
|   |-- postprocess_dataframe.py
|
|-- m5_save_output
|   |-- remove_ckpt.py
|   |-- save_output.py
|
|-- m6_testing
```