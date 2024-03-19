#!/bin/bash
#SBATCH --job-name=run_test
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/test_run.out
#SBATCH --time=24:00:00
#SBATCH -p interactive-gpu
#SBATCH --gres=gpu:a40:1
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

source ~/.bashrc
cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage

module load python3
conda activate ta2

python3 -m fusemine -d /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/ARCHIVE/data/raw