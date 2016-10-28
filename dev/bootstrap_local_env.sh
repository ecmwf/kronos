#!/bin/sh

cd ${bamboo_working_directory}

# Ensure that our python environment is clean

unset PYTHONPATH

# Get conda, and install it (very) locally

wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
sh Miniconda2-latest-Linux-x86_64.sh -b -p ${bamboo_working_directory}/miniconda
export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}

# Install the testing environment!

conda install pyyaml
conda env create -n test_env -f conda_environment.txt

