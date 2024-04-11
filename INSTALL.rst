=================
Installing Kronos
=================


Kronos Executor
===============

1. **Get and install ``conda`` (if not available on the system):**

   The easiest way to install the required python dependencies is using the ``conda`` package
   management system. If this is not available in your system it may be installed in a local
   directory as follows::

      wget "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
      bash Miniforge3-$(uname)-$(uname -m).sh
      unset PYTHONPATH
      export PATH=$PWD/miniforge3/bin:$PATH

2. **Get Kronos sources**::

      git clone https://github.com/ecmwf/kronos

3. **Create conda environment for the executor**::

      cd kronos
      conda env create -n kronos_executor_env -f kronos_executor/conda_env_executor.yml

4. **Install the Kronos Executor**::

      source activate kronos_executor_env
      cd kronos_executor
      pip install -e .

At this stage, Kronos-Executor should be installed under the “executor” conda environment and all
the appropriate environment settings should be automatically detected. It is possible to verify
the correct installation of the Kronos Executor by running::

   source activate kronos_executor_env
   conda list

``kronos-executor`` should now appear in the list of packages available in the executor
environment.


Synthetic Apps
==============

This step installs the synthetic apps. Building the executable requires:

1. A POSIX 2004 system

2. A working C compiler

3. A working installation of MPI

4. CMake (version 3.6 or higher)

5. ecBuild (version 3.4 or higher, https://github.com/ecmwf/ecbuild)

To build the synthetic apps, create a build directory outside the source tree::

   mkdir kronos-build
   cd kronos-build
   ecbuild <path-to-kronos-source>
   make

If the synthetic apps are successfully installed, an executable called "kronos-synapp" is generated
in kronos-build/bin

