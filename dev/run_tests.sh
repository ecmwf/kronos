#!/bin/sh

# Get access to the installed python environment

export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}

source activate test_env

cd ${bamboo_working_directory}/src
python ${bamboo_working_directory}/miniconda/envs/test_env/lib/python2.7/site-packages/pytest.py --junitxml="${bamboo_working_directory}/test_output.xml" tests
