

#!/bin/bash
# (C) Copyright 1996-2018 ECMWF.
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

# Work around ~/.conda being incorrectly (hard)-used in cond a create.

export HOME="${bamboo_working_directory}"

# Work around conda test against nonexistent variable tripping set -e
export CONDA_PATH_BACKUP=""
export PS1=""

# Install the testing environment!
conda install -y pyyaml


# ========= environment for the modeller =========
cd $HOME/kronos_modeller
if [[ -f conda_env_modeller.yml ]]; then
    conda env create -n test_env -f conda_env_modeller.yml
fi

# install the executor+modeller
cd $HOME
source activate test_env
pip install -e kronos_executor
pip install -e kronos_modeller


# ========= environment for the executor =========
cd $HOME/kronos_executor
if [[ -f conda_env_executor.yml ]]; then
    conda env create -n test_env_exe -f conda_env_executor.yml
fi

# install the executor
cd $HOME
source activate test_env_exe
pip install -e kronos_executor

