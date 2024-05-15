# FuseMine
## Overview
FuseMine is a pipeline designed for data reconciliation across multiple mineral site databases (e.g., USMIN and MRDS) and mineral sites extracted from reports (e.g., output from TA2 InferLink/USC). FuseMine facilitates the reconciliation of mineral sites by leveraging both location-based data and semantic similarities in textual attributes such as site name and commodity.

## Installation
1. Clone this repository
```
git clone https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage.git
cd umn-ta2-mineral-site-linkage
```

2. FuseMine recommends allocating more than 10GB of memory. The code requires `python >= 3.10` and `pytorch >= 2.0`. The following is the example command line for creating a conda environment with `fusemine` name and `python=3.10`

```
conda create -n fusemine python=3.10
conda activate fusemine
```

3. Installation instructions for pytorch can be found [here](https://pytorch.org/get-started/locally/). Installing with CUDA support is highly recommended but not required for running FuseMine.


4. Install the required package libraries by running the following command line:
```
pip install -r requirements.txt
```

## Usage
### FuseMine Arguments

The following command-line arguments are available to customize FuseMine:

- `--raw_data`: Directory or file where the raw mineral site databases are located.
- `--attribute_map`: CSV file with label mapping information (see sample_mapfile.csv for reference).
- `--schema_output_directory`: Directory where the processed raw mineral site database will be stored.
- `--schema_output_filename`: Filename for the processed raw mineral site database.
- `--commodity`: Specific commodity to focus on.
- `--single_stage`: Method for location-based single-stage linking (default: `distance`).
- `--intralink`: Method for location-based intralinking (default: `distance`).
- `--interlink`: Method for location-based interlinking (default: `area`).
- `--same_as_directory`: Directory to store the same as CSV files (default: `./output`).
- `--same_as_filename`: Filename of the same as CSV file (default: `./<commodity>_same_as.csv`).
- `--tungsten`: Run evaluation with tungsten.

## Example FuseMine Commands
Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). 

To populate additional raw structured data to MinMod KG, create the attribute map file (.CSV), which maps headers of raw structured data to [MinMod KG schema](https://github.com/DARPA-CRITICALMAAS/schemas/tree/main/ta2). Here is an example of attribute map CSV: [`sample_mapfile.csv`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv).

After creating a attribute map CSV, run the following command line:
```
python3 fusemine.py --raw_data <path/to/raw/data> --attribute_map <path/to/attribute/map> --schema_output_directory path/to/schema/directory --schema_output_filename file_name
```

To store the output file in the default location (./outputs/) and use distance-based inra-linking and area-based inter-linking methoeds, use the following command:
```
python3 fusemine.py -commodity <commodity_name> --intralink distance --interlink area
```

To evaluate the performance of FuseMine on Idaho/Montana region Tungsten assessment data[( et al., )](), use the following:
```
python3 fusemine.py --tungsten
```

## FuseMine Parameters
The following table lists all the parameters used in the pipeline. These values can be modified in the [`params.ini`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/params.ini).

| Name | Description | Value |
| --- | --- | --- |
| `POINT_BUFFER_unit_meter` | Maximum distance between two geocoordinate points to be considered as a same site | 2500 (meters) |
| `POLYGON_BUFFER_unit_meter` | Size of buffer to be added around a geometry | 5000 (meters) |
| `POLYGON_AREA_OVERLAP_unit_sqmeter` | Minimum intersection-over-union (IOU) two geometries must have to be considered as a same site | 0.5 |
| `ATTRIBUTE_VALUE_THRESHOLD` | Minimum cosine similarity distance two text embeddings must have to be considered as a similar text | 0.65 |


<!-- Raw data process requires an attribute map which is structured as follows:
| attribute_label | corresponding_attribute_label | file_name |
| --- | --- | --- |
| target attribute label required by mineral site schema | attribute label in the raw data | file_name |

Consider the following when creating the attribute map file:
- If the attribute spans across multiple files (e.g., `record_id` is available in `A.csv` and `commodity` is available in `B.csv`), please indicate the corresponding file name in the `file_name` field.
- If there are multiple attributes in the raw data representing the same target attribute (e.g., both `commod1` and `commod2` represents `commodity`), please indicate all attributes on separate rows with identical `attribute_label`.
- If the attribute is not availble in the raw data (e.g., `crs` is EPSG:4326 but there is no column representing `crs` in the data), fill in the `corresponding_attribute_label` with the required information, but leave `file_name` empty. -->

<!-- If the attribute spans across multiple files /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/sample_mapfile.csv -->

<!-- ### Run Interlinking on Intralinked Data -->


<!-- ground reference -->
