#!/bin/sh

# Get access to the installed python environment

export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}
source activate test_env

find ${bamboo_working_directory} -type f -name "*.py" -print0 | xargs -0 -n1 pyflakes;
