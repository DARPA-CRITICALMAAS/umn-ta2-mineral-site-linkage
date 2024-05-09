# TA2 UMN - FuseMine (UPDATE CURRENTLY IN PROGRESS)
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
### Run FuseMine End-to-End
Run the python file by entering the following code in the command line:
```
python3 fusemine.py --commodity commodity_name --intralink distance --interlink area
```
The final output, formatted as a two-column CSV, will be found in the directory `./outputs/` with the name `{commodity_name}_sameas.csv` by default.

To save the same as result to a specific directory with a specific filename, append the following flags, respectively, with the desired values:
```
--same_as_directory path/to/directory --same_as_filename file_name
```

<!-- ### Run Interlinking on Intralinked Data -->

### Parameters
The following portion lists all the parameters that are used in the pipeline. These values can be modified in the [`params.ini`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/params.ini) file.

| Name | Description | Value |
| --- | --- | --- |
| `POINT_BUFFER_unit_meter` | Maximum distance between two geocoordinate points to be considered as a same site | 2500 (meters) |
| `POLYGON_BUFFER_unit_meter` | Size of buffer to be added around a geometry | 5000 (meters) |
| `POLYGON_AREA_OVERLAP_unit_sqmeter` | Minimum intersection-over-union (IOU) two geometries must have to be considered as a same site | 0.5 |
| `ATTRIBUTE_VALUE_THRESHOLD` | Minimum cosine similarity distance two text embeddings must have to be considered as a similar text | 0.65 |
