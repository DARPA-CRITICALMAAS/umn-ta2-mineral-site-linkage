# UMN TA2 Mineral Site Linkage
This repository is for the reconciliation of geospatial datasets (e.g., mineral site databases - MRDS, USMIN)

## Requirements
Install the required package libraries by running the following code in the command line:
```
pip install -r requirements.txt
```

## How to run
### Run End-to-End
Run the python file by entering the following code in the command line:
```
python fusemine.py [-d path/to/data/directory] [-l if only using geolocation for linking]
```

### Upload Raw Database to MinMod Knowledge Graph
```
cd m1_preprocessing
python intralinking.py -d path/to/data/directory [-l if only using geolocation for linking]
```


### Run Intralinking Model
Use the following code if you are using mineral site data in local storage:
```
cd m2_intralinking
python intralinking.py -d path/to/data/directory [-l if only using geolocation for linking]
```

Use the following code if you are using mineral site data on the knowledge graph:
```
cd m3_interlinking
python intralinking.py [-l if only using geolocation for linking]
```

### Run Interlinking Model

## Directory Layout
```
./
|-- fusemine.py
|
|-- m0_loading_and_saving
|   |-- loading_local_data.py               # Loads the data available on the user local storage
|   |-- loading_kg_data.py                  # Loads the data available on the knowledge graph
|   |-- save_to_geojson_output.py           # Saves the output as a geojson file that can plotted on a GIS software
|   |-- save_to_json_output.py              # Saves the output as a json file that can be loaded on the knowledge graph
|
|-- m1_preprocessing
|   |-- process_gpkg_to_json.py
|   |-- process_rawdb_to_schema.py
|
|-- m2_intralinking
|   |-- intralinking.py
|   |-- location_based_intralinking.py
|   |-- text_based_intralinking.py
|
|-- m3_interlinking
|   |-- interlinking.py
|   |-- location_based_intralinking.py
|
|-- m4_postprocessing
|
|-- resource
|   |-- crs.pkl                             # List of coordinate reference systems (CRS)

```

### Parameters
The following portion lists all the parameters that are used in the pipeline. These values can be modified in the `params.ini` file.

| Name | Description | Value |
| --- | --- | --- |
| `INTRALINK_BOUNDARY` | Intralink Boundary | 0.5 (kilometers) |
| `INTERLINK_BUFFER` | Interlink Buffer | 15 (meters) |
| `INTERLINK_OVERLAP` | Interlink Overlap Area | 1 (meters squared) |
| `THRESHOLD_SIMILARITY` | Textual Similarity Threshold | 0.74 |
<!-- | `EMBEDDING_RATIO1` | Ratio of Name Embedding | 0.71 |
| `EMBEDDING_RATIO2` | Ratio of Commodity Embedding | 0.29 | -->