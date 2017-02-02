=============
INSTALL
=============

Quick Install
---------------

Easiest way to install the required dependencies for Kronos is through the python package management system *conda*

1. get and install conda

  > wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

  > sh Miniconda2-latest-Linux-x86_64.sh -b -p ${working_directory}/miniconda

2. set environment

  > unset PYTHONPATH

  > export PATH=${working_directory}/miniconda/bin:${PATH}

3. Create the kronos environment from requirements file (conda_environment.txt)

  > conda install -y pyyaml

  > conda env create -n kronos -f conda_environment.txt

===============================================================================

4. To activate the kronos environment in conda:

  > source activate kronos
