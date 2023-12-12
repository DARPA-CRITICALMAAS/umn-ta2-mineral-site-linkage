#!/bin/bash
#SBATCH --job-name=xfrmr_test_indiv
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/run_results/test_individual.out
#SBATCH --time=8:00:00
#SBATCH -p amd2tb
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=NONE
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load python3
python3 minelink/intiation_modules/check_individual.py