=======
INSTALL
=======

Quick Install
-------------

Easiest way to install the required dependencies for Kronos is through the python package management system *conda*.

1. Get and install *conda*
~~~~~~~~~~~~~~~~~~~~~~~~~~

  > wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

  > sh Miniconda2-latest-Linux-x86_64.sh -b -p ${working_directory}/miniconda

2. Set environment
~~~~~~~~~~~~~~~~~~

  > unset PYTHONPATH

  > export PATH=${working_directory}/miniconda/bin:${PATH}

3. Create "kronos" environment from conda_environment.txt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  > conda install -y pyyaml

  > conda env create -n kronos -f conda_environment.txt

4. Activate "kronos" environment in conda:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  > source activate kronos

5. Install Kronos-core (model)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  > mkdir {working_directory}/kronos

  > cd {working_directory}/kronos

  > pip install .

6. Install Kronos-executor
~~~~~~~~~~~~~~~~~~~~~~~~~~

  > mkdir {working_directory}/kronos-build

  > cd {working_directory}/kronos-build

  > cmake {working_directory}/kronos

  > make