#!/bin/bash
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

set -uex

cd ${bamboo_working_directory}

# Ensure that our python environment is clean

unset PYTHONPATH

# Get conda, and install it (very) locally

wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
sh Miniconda2-latest-Linux-x86_64.sh -b -p ${bamboo_working_directory}/miniconda
export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}

# Install the testing environment!

HOME="${bamboo_working_directory}" conda install -y pyyaml

if [[ -f conda_environment.txt ]]; then
    HOME="${bamboo_working_directory}" conda env create -n test_env -f conda_environment.txt
fi

# Make python packages cloned into the depends directory available to pip

source activate test_env
find ./depends -maxdepth 1 -mindepth 1 -type d -exec pip install -e {} \;
