{\rtf1\ansi\ansicpg1252\cocoartf1671\cocoasubrtf600
{\fonttbl\f0\fnil\fcharset0 Monaco;}
{\colortbl;\red255\green255\blue255;\red22\green21\blue22;\red22\green21\blue22;\red214\green0\blue72;
\red22\green21\blue22;\red0\green0\blue0;}
{\*\expandedcolortbl;;\cssrgb\c11373\c10980\c11373;\cssrgb\c11373\c10980\c11373\c3922;\cssrgb\c87843\c11765\c35294;
\cssrgb\c11373\c10980\c11373\c3922;\csgray\c0;}
\margl1440\margr1440\vieww21580\viewh13780\viewkind0
\deftab720
\pard\pardeftab720\sl360\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
#!/bin/sh\
\
\pard\pardeftab720\sl320\partightenfactor0
\cf4 \cb5 \outl0\strokewidth0 \strokec4 #SBATCH --partition=slurm\cf2 \cb3 \outl0\strokewidth0 \
\pard\pardeftab720\sl360\partightenfactor0
\cf2 #SBATCH \'97A IHESD\
#SBATCH --nodes=1\
#SBATCH --time=64:01:00\
\
\
# README -----------------------------------------------------------------------\
#\
# This script will launch SLURM tasks that will execute \cf6 \cb1 \kerning1\expnd0\expndtw0 \CocoaLigature0 /pic/projects/GCAM/stitches_pic/stitches_repo/stitches/modules/run_paper_experiment_scenarioMIP.py\cf2 \cb3 \expnd0\expndtw0\kerning0
\CocoaLigature1  \
#\
# To execute this script run the following:\
#\
# `sbatch \cf6 \cb1 \kerning1\expnd0\expndtw0 \CocoaLigature0 /pic/projects/GCAM/stitches_pic/stitches_repo/stitches/modules/run_paper_experiment_scenarioMIP.sh\cf2 \cb3 \expnd0\expndtw0\kerning0
\CocoaLigature1 `\
#\
# ------------------------------------------------------------------------------\
\
# purge other modules because anaconda could conflict with other loaded Python versions\
module purge\
\
# load the parent version of Python \
module load \cf6 \cb1 \kerning1\expnd0\expndtw0 \CocoaLigature0 python/miniconda3.8 \cf2 \cb3 \expnd0\expndtw0\kerning0
\CocoaLigature1 \
\
# source the profile\
source \cf6 \cb1 \kerning1\expnd0\expndtw0 \CocoaLigature0 /people/snyd535/miniconda3/etc/profile.d\cf2 \cb3 \expnd0\expndtw0\kerning0
\CocoaLigature1 /conda.sh\
\
# activate Python virtual environment\
conda activate \cf6 \cb1 \kerning1\expnd0\expndtw0 \CocoaLigature0 /people/snyd535/miniconda3/envs\cf2 \cb3 \expnd0\expndtw0\kerning0
\CocoaLigature1 /pangeo\
\
# run the python script you built \
python \cf6 \cb1 \kerning1\expnd0\expndtw0 \CocoaLigature0 /pic/projects/GCAM/stitches_pic/stitches_repo/stitches/modules/paper_experiment_scenarioMIP.py\cf2 \cb3 \expnd0\expndtw0\kerning0
\CocoaLigature1 \
}