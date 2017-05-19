=======
INSTALL
=======


Kronos Modeller
---------------

The Kronos Modeller component ingests profiled workload data (KPf file) and generates a workload model and schedule (KSF file). The generated schedule can then be read by
the Kronos Executor that deploys the schedule on a HPC system for benchmarking purposes. The python packages used by the Kronos Modeller are largely shared with the Kronos Executor component. Here below a step-by-step guide for installing the Kronos Modeller is provided.

1. **Get and install *conda* from sources (if not available on the system):**
  The easiest way to install the required python dependencies is using the *conda* package management system. If this is not available in your system it may be installed
in a local directory as follows:

  > wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

  > sh Miniconda2-latest-Linux-x86_64.sh -b -p ${working_directory}/miniconda

  > unset PYTHONPATH

  > export PATH=${working_directory}/miniconda/bin:${PATH}

2. **Extract Kronos sources**

  > cd {working_directory}

  > cp kronos-<version>-Source.tar.gz .

  > tar xzvf kronos-<version>-Source.tar.gz

3. **Create conda environment for modeller**

  > cd kronos-<version>-Source

  > conda env create -n modeller -f conda_environment.txt

4. **Install Kronos Modeller**

  > source activate modeller

  > cd kronos-<version>-Source

  > pip install .

  At this stage, Kronos Modeller should be installed under the “modeller” conda environment and all the appropriate environment settings should be automatically detected.
It is possible to verify the correct installation of the Kronos Modeller by running:

    > source activate modeller

    > conda env export

  The name of kronos=<version> should now appear in the list of packages available in the modeller environment.
