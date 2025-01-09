# FuseMine: Mineral Site Record Linking

## Overview
FuseMine is a comprehensive pipeline designed to link data across multiple mineral site databases, such as USMIN and MRDS, and mineral site records extracted from reports and tables through tools created by TA2 Inferlink and USC. It facilitates the reconciliation process by utilizing both geospatial data and semantic similarities in textual attributes, such as site name and commodity. 

## Requirements
To run FuseMine, ensure the following requirements are met:
- **Hardware**: A local device with GPU support is recommended.
- **Software**: Docker must be installed. Follow the installation steps on the official [Docker website](https://docs.docker.com/engine/install/).
- **Memory**: At least 10 GB of available memory is suggested (actual requirements depend on the data size).
- **Network**: An active internet connection is required during execution.

FuseMine simplifies usage by providing shell scripts for common workflows, including processing raw data into schema format and linking mineral sites. These scripts:
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
poetry run python3 fusemine.py --commodity <commodity>
```
- `commodity`: The specific commodity to focus on. The commodity name must match one of the MRDS commodity names listed [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv).

3. Move data from Docker container to local
```
exit
docker cp <container_id>:/umn-ta2-mineral-site-linkage/outputs/<commodity>_sameas.csv <save_path>
```
- `save_path`: Path of directory to store the same_as links

4. Stop Docker container
```
docker stop <container_id>
```

FuseMine generates a log file that reports the number of available records on the MinMod KG, the number of identified mineral sites, and the run time. These log files are saved in the `./logs` directory.

Once the data is merged into the main branch, it typically takes 5 to 10 minutes for the changes to be reflected on the KG.