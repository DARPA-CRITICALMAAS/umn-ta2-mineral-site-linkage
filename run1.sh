#!/bin/bash
#SBATCH --job-name=preprocessing_data
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/preprocessing_data.out
#SBATCH --time=50:00:00
#SBATCH -p ag2tb
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load python3
python3 -m minelink -d /home/yaoyi/pyo00005/CriticalMAAS/src/data/raw -s /home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/inferlink/extractions
python3 mineralsite.py