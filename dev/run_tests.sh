#!/bin/bash
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

set -uex

# Get access to the installed python environment

export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}

source activate test_env

cd ${bamboo_working_directory}

for p in `find -maxdepth 3 -mindepth 1 -type d -name tests -not -path "./kronos-synapps/*"`; do
    PYTHONPATH=`pwd` python ${bamboo_working_directory}/miniconda/envs/test_env/lib/python2.7/site-packages/pytest.py --junitxml="${bamboo_working_directory}/test_output.xml" ${p}
done
