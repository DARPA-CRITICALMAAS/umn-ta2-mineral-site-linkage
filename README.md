# FuseMine: Mineral Site Record Linking

## Overview
FuseMine is a comprehensive pipeline designed to link data across multiple mineral site databases, such as USMIN and MRDS, and mineral site records extracted from reports and tables through tools created by TA2 Inferlink and USC. It facilitates the reconciliation process by utilizing both geospatial data and semantic similarities in textual attributes, such as site name and commodity. 

## Requirements
To run FuseMine, ensure the following requirements are met:
- **Hardware**: A local device with GPU support is recommended.
- **Software**: Docker and Git Large File Storage must be installed. Follow the installation steps on the official [Docker website](https://docs.docker.com/engine/install/) and [Git LFS](https://docs.github.com/en/repositories/working-with-files/managing-large-files/installing-git-large-file-storage).
- **Memory**: At least 10 GB of available memory is suggested (actual requirements depend on the data size).
- **Network**: An active internet connection is required during execution.

FuseMine simplifies usage by providing shell execution script:
1. Clone the [MinMod data repository](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data) (if not already cloned).
2. Create and run a Docker container.
3. Export the resulting data from the container
4. Push the data to the data repository.

Ensure you are running these scripts on a system capable of executing Bash scripts. If you would like to run through Docker, use follow the commands under Command (with Docker).

## Installation
1. Clone the repository
```
git clone https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage.git
cd umn-ta2-mineral-site-linkage
```

## Usage
Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). The last update time and version of MinMod KG can be checked [here](https://minmod.isi.edu/resource/kg).

### Running FuseMine (with Shell Script)
```
./run_linking.sh <commodity>
```
- `commodity`: The specific commodity to focus on. The commodity name must match one of the MRDS commodity names listed [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv).    
<!-- TODO: replace -->

### Running FuseMine (with Docker)
1. Build Docker container
```
docker build -t ta2-linking .
docker run -dit ta2-linking
docker exec -it <container_id> /bin/bash
```
- `container_id`: Container ID of the created Docker container. Can be found by running `docker ps`

2. Run the command
```
poetry run python3 run_fusemine.py --commodity <commodity>
```
- `commodity`: The specific commodity to focus on. The commodity name must match one of the MRDS commodity names listed [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv).

3. Move data from Docker container to local
```
exit
docker cp <container_id>:/umn-ta2-mineral-site-linkage/output/<commodity>_<date>_sameas.csv <save_path>
```
- `save_path`: Path of directory to store the same_as links

4. Stop Docker container
```
docker stop <container_id>
```

FuseMine generates a log file that reports the number of available records on the MinMod KG, the number of identified mineral sites, and the run time. These log files are saved in the `./logs` directory.

Once the data is merged into the main branch, it typically takes 5 to 10 minutes for the changes to be reflected on the KG.

## Evaluation

FuseMine has been evaluated using the following three datasets:

- [Spatial data associated with tungsten skarn resource assessment of the Northern Rocky Mountains, Montana and Idaho](https://www.usgs.gov/data/spatial-data-associated-tungsten-skarn-resource-assessment-northern-rocky-mountains-montana)
    - **States**: Idaho, Montana
    - **Focus Commodity**: Tungsten
    - **Data Path**: [`./tests/eval_data/w_idmt.csv`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/tests/eval_data/w_idmt.csv)
- [Tungsten skarn mineral resource assessment of the Great Basin region of western Nevada and eastern California - Geodatabase](https://www.usgs.gov/data/tungsten-skarn-mineral-resource-assessment-great-basin-region-western-nevada-and-eastern-0)
    - **States**: Nevada, California
    - **Focus Commodity**: Tungsten
    - **Data Path**: [`./tests/eval_data/w_gbw.csv`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/tests/eval_data/w_gbw.csv)
- Nickel in Upper Midwest United States
    - **States**: Michigan, Minnesota, Wisconsin
    - **Focus Commodity**: Nickel
    - **Data Path**: [`./tests/eval_data/ni_umidwest.csv`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/tests/eval_data/ni_umidwest.csv)

The following table summarizes the model's performance, evaluated based on Accuracy and [Macro-averaged F1 Score](https://en.wikipedia.org/wiki/F-score#Macro_F1):

| Dataset | Accuracy (%) | Macro-averaged F1 Score (%) | Count Correct Links | Count Correct Non-links |
| --- | --- | --- | --- | --- |
| `w_idmt` | 99.95 | 70.57 |  |  |
| `w_gbw` | 99.89 | 72.98 |  |  |
| `ni_umidwest` |  |  |  |  |