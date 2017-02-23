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
source activate test_env

cd ${bamboo_working_directory}

find . -path ./.git -prune -o \
       -path ./miniconda -prune -o \
       -path ./depends -prune -o \
       -type f -name "*.py" -print0 | xargs -0 -n1 pyflakes;

source deactivate
