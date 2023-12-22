#!/bin/bash
#SBATCH --job-name=ab_reduction
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/run_results/reduction_ab.out
#SBATCH --time=24:00:00
#SBATCH -p amd2tb
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load mamba
module load python3
conda activate ta2
python3 -m minelink.ab_run -d /home/yaoyi/pyo00005/CriticalMAAS/src/data/raw