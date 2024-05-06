#!/bin/bash
#SBATCH --job-name=4fusemine_main
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/fusemine_main4.out
#SBATCH --time=24:00:00
#SBATCH -p a100-4
#SBATCH --gres=gpu:a100:1 
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=None
#SBATCH --mail-user=pyo00005@umn.edu

module load python3
source ~/.bashrc
conda activate ta2
cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage
python3 test.py -c nickel
# python3 fusemine.py data_process -d /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/full_MRDS.csv -m /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/sample_map_file.csv