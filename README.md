# TA2 UMN - FuseMine
FuseMine is the pipeline for data reconciliation from multiple mineral site databases (e.g., USMIN and MRDS) and mineral sites extracted from the reports (i.e., output from TA2 InferLink/USC).

## Requirements
### Conda Environment Configurations
FuseMine recommends dedicating more than 10GB of memory. The code requires `python >= 3.10` and `pytorch >= 2.0`.

The following is the example command line for creating a conda environment with `fusemine` name and `python=3.10`

```
conda create -n fusemine python=3.10
conda activate fusemine
```

Installation instructions for pytorch can be found [here](https://pytorch.org/get-started/locally/). Installing with CUDA support is highly recommended but not required for running FuseMine.

Install the required package libraries by running the following command line:
```
pip install -r requirements.txt
```

## How to run
### Processing Raw Data to Mineral Site Schema
Before running FuseMine, you'll need to check that all data are on the [MinMod Knowledge Graph](https://minmod.isi.edu/). The [required attribute](https://github.com/DARPA-CRITICALMAAS/schemas/tree/main/ta2) can be across multiple files as long as they are indicated in the attribute map. An example of an acceptable attribute map can be found [here](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv).
```
python3 fusemine.py --raw_data path/to/raw/data --attribute_map path/to/attribute/map --sod path/to/schema/directory --sof file_name
```

### Run FuseMine
Enter one of the following in the command line to run FuseMine:
```
# Store same as file as ./outputs/{commodity_name}_sameas.csv
python3 fusemine.py -c commodity_name --intralink distance --interlink area

# Store same as file at a specific directory with a specific name
python3 fusemine.py -c commodity_name --intralink distance --interlink area -d path/to/directory -f file_name
```

#### Parameters
The following portion lists all the parameters that are used in the pipeline. These values can be modified in the [`params.ini`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/params.ini) file.

| Name | Description | Value |
| --- | --- | --- |
| `POINT_BUFFER_unit_meter` | Maximum distance between two geocoordinate points to be considered as a same site | 2500 (meters) |
| `POLYGON_BUFFER_unit_meter` | Size of buffer to be added around a geometry | 5000 (meters) |
| `POLYGON_AREA_OVERLAP_unit_sqmeter` | Minimum intersection-over-union (IOU) two geometries must have to be considered as a same site | 0.5 |
| `ATTRIBUTE_VALUE_THRESHOLD` | Minimum cosine similarity distance two text embeddings must have to be considered as a similar text | 0.65 |

### Run FuseMine with Evaluation
To evaluate the performance of FuseMine on [Idaho/Montana region Tungsten](), use the following:
```
python3 fusemine.py --tungsten
```


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
