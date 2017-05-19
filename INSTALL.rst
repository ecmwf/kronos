=======
INSTALL
=======


Kronos Executor
---------------

The Kronos Executor is part of the Kronos package. The easiest way to install the required python dependencies is using the *conda* package management system.
If this is not available through your package management system it may be installed in a local directory as follows:

1. **Get and install *conda* from sources (if not available on the system):**

  > wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

  > sh Miniconda2-latest-Linux-x86_64.sh -b -p ${working_directory}/miniconda

  > unset PYTHONPATH

  > export PATH=${working_directory}/miniconda/bin:${PATH}

2. **Extract Kronos sources**

  > cd {working_directory}

  > cp kronos_v<version>.tar.gz .

  > tar xzvf kronos_v<version>.tar.gz

3. **Create python environment for executor**

  > conda env create -n executor -f conda_environment_exe.txt

4. **Install Kronos Executor**

  > source activate executor

  > cd {working_directory}

  > pip install .

  At this stage, Kronos-Executor should be installed under the “executor” environment and all the
  appropriate environment settings should be automatically detected. It is possible to verify the correct installation of the Kronos-Executor by running:

    > source activate executor

    > conda env export

  The name of kronos=<version> should now appear in the list of packages available in the executor environment.

5. **Install the Synthetic Apps**

  Building this executable will require:

    1. A working C compiler

    2. A working installation of MPI compatible with the C compiler

    3. CMake (version 2.8.11 or higher).

  To build kronos-coordinator, create a build directory outside the source tree:

  > mkdir {kronos-build}

  > cd {kronos-build}

  > cmake <path-to-kronos-source>

  > make

If the synthetic apps are successfully installed, an executor called "kronos-coordinator" is generated in {kronos-build}/bin

~~~~~~~~~~~~~~~
Kronos-Modeller
~~~~~~~~~~~~~~~
The python components of the Kronos Modeller are largely shared with the Kronos Executor. The installation steps
for the Kronos-Executor should be followed. If the modeller is not to be used on the same system the reduced set of
dependencies in *conda_environment_exe.txt* may be used -> the "executor" conda environment should be created
