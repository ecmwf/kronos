=======
INSTALL
=======

-------------
Quick Install
-------------

~~~~~~~~~~~~~~~
Kronos Modeller
~~~~~~~~~~~~~~~
The Kronos Modeller is part of the Kronos package, and is formed of the Kronos python library and the Kronos-modeller executable. The easiest way to install the required dependencies is using the *conda* package management system. If this is not available through your package management system it may be installed in a local directory as follows:

1. Get and install *conda* from sources:

  > wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

  > sh Miniconda2-latest-Linux-x86_64.sh -b -p ${working_directory}/miniconda

  > unset PYTHONPATH

  > export PATH=${working_directory}/miniconda/bin:${PATH}

Given conda, the Kronos package can now be made available:

2. Extract Kronos “tarball” into your working directory:

  > cd {working_directory}

  > cp kronos_V0.1.0.tar.gz .

  > tar xzvf kronos_V0.1.0.tar.gz

3. Create "kronos" environment from kronos-core/conda_environment.txt file as follows:

  > conda env create -n kronos -f conda_environment.txt

4. Activate "kronos" environment in conda

  > source activate kronos

5. Install pip within the "kronos" environment in conda

  > conda install pip

6. Install Kronos

  > cd {working_directory}/kronos-core

  > pip install .

At this stage, Kronos-Modeller should be installed under the “kronos” *conda* environment and all the appropriate environment settings should be automatically detected.

It is possible to verify the correct installation of Kronos by running:

  > source activate kronos

  > conda env export

The name of kronos=0.0.1 should now appear in the list of packages available in the kronos environment.


~~~~~~~~~~~~~~~
Kronos-Executor
~~~~~~~~~~~~~~~
The python components of the Kronos Executor are largely shared with the Kronos Modeller. The installation steps
for the Kronos-Modeller should be followed. If the modeller is not to be used on the same system the reduced set of
dependencies in *conda_environment_exe.txt* may be used.

The synthetic applications make use of a binary executable called kronos-coordinator. Building this executable will require:

1. A working C compiler

2. A working installation of MPI compatible with the C compiler

3. CMake (version 2.8.11 or higher).

To build kronos-coordinator, create a build directory outside the source tree:

  > mkdir {kronos-build}

  > cd {kronos-build}

  > cmake <path-to-kronos-source>

  > make
