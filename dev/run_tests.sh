#!/bin/bash
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

set -uex

# Work around conda test against nonexistent variable tripping set -e

export CONDA_PATH_BACKUP=""
export PS1=""

# Get access to the installed python environment

export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}

# tests of the modeller

cd ${bamboo_working_directory}/kronos/core
source activate test_env
for p in `find -maxdepth 3 -mindepth 1 -type d -name tests -not -path "./kronos-synapps/*"`; do
    PYTHONPATH=`pwd` python ${bamboo_working_directory}/miniconda/envs/test_env/lib/python2.7/site-packages/pytest.py --junitxml="${bamboo_working_directory}/test_output.xml" ${p}
done
source deactivate


# tests of the executor

cd ${bamboo_working_directory}/kronos/executor
source activate test_env_exe
for p in `find -maxdepth 3 -mindepth 1 -type d -name tests -not -path "./kronos-synapps/*"`; do
    PYTHONPATH=`pwd` python ${bamboo_working_directory}/miniconda/envs/test_env/lib/python2.7/site-packages/pytest.py --junitxml="${bamboo_working_directory}/test_output_exe.xml" ${p}
done
source deactivate