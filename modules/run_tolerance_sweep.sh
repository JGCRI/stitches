#!/bin/sh

#SBATCH --partition=slurm
#SBATCH --time=1:01:00
#SBATCH -n 1
#SBATCH -A IHESD


# README -----------------------------------------------------------------------
#
# This script will launch SLURM tasks that will execute /pic/projects/GCAM/stitches_pic/stitches_repo/stitches/modules/run_paper_experiment_scenarioMIP.py 
#
# To execute this script run the following:
#
# `sbatch /pic/projects/GCAM/stitches_pic/stitches_repo/stitches/modules/run_tolerance_sweep.sh`
#
# ------------------------------------------------------------------------------

# purge other modules because anaconda could conflict with other loaded Python versions
module purge


# load the parent version of Python 
module load python/miniconda3.8 

# source the profile
source /people/snyd535/miniconda3/etc/profile.d/conda.sh

# activate Python virtual environment
conda activate /people/snyd535/miniconda3/envs/pangeo

# run the python script you built 
python /pic/projects/GCAM/stitches_pic/stitches_repo/stitches/modules/paper_experiment_tolerance_sweep.py

# update permissions of the outputs
chmod -R 777 /pic/projects/GCAM/stitches_pic/paper1_outputs/*
