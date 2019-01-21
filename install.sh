#!/bin/bash

set -o nounset

KRONOS_VERSION=0.6.0
KRONOS_PACKAGE=kronos-${KRONOS_VERSION}-Source

WORK_DIR=$(dirname $(readlink -f $BASH_SOURCE))
DEPENDS_DIR=${WORK_DIR}/depends
CONDA_DIR=${WORK_DIR}/miniconda
CONDA_BIN_DIR=${CONDA_DIR}/bin
CONDA_CMD=${CONDA_BIN_DIR}/conda
CONDA_INSTALLER_EXE=Miniconda2-latest-Linux-x86_64.sh

# ====== print help documentation ======
print_help() {

  echo -e "\nUSAGE:\n"
  echo -e "--modeller : install the modeller python package (with all required dependencies)"
  echo -e "--executor : install the executor python package (with all required dependencies)"
  echo -e "--synapps  : install kronos synthetic-apps"
  echo -e "--offline  : conda and kronos dependencies are installed offline"
  echo -e "--all      : install conda, kronos-executor, kronos-modeller, synthetic apps"
  echo -e "--help     : show this help"

}

# === download and install the conda installer ====
install_conda() {

    local isoffline=$1

    if [[ $isoffline == 1 ]]; then

        # install offline (it require the depends directory with all the dependencies in it)
        local conda_inst_offline=${DEPENDS_DIR}/${CONDA_INSTALLER_EXE}
        if [[ ! -f ${conda_inst_offline} ]]; then
            echo "Asked to install conda offline but ${conda_inst_offline} not found!"
            exit 1
        else
            set -e
            echo "Found conda in /depends: installing.."
            sh ${conda_inst_offline} -b -p ${WORK_DIR}/miniconda
            set +e
        fi

    else
        # download miniconda from website (exit if errors)
        set -e
        wget -c http://repo.continuum.io/miniconda/${CONDA_INSTALLER_EXE} -P ${WORK_DIR}
        sh ${WORK_DIR}/${CONDA_INSTALLER_EXE} -b -p ${CONDA_DIR}
        set +e
    fi

}


# === install the executor ===
install_executor() {

    local isoffline=$1

    # check that conda is already installed
    if [[ ! -d ${CONDA_DIR} ]]; then
        echo "Miniconda missing, install conda first.."
        exit 1
    fi

    # if executor is already installed, return
    if [[ -d ${CONDA_DIR}/envs/kronos_environment ]]; then
        echo "kronos_environment already exists, installing executor.."
        export PATH=${CONDA_BIN_DIR}/:${PATH}
        source activate kronos_environment
        pip install -e ${WORK_DIR}/kronos_executor
    else

        # export conda command
        export PATH=${CONDA_BIN_DIR}/:${PATH}

        # install the conda dependencies first
        if [[ $isoffline == 1 ]]; then

            echo "installing executor dependencies (offline).."

            # create an empty environment
            conda create -y -n kronos_environment --offline
            source activate kronos_environment

            # Install the kronos-executor dependencies
            conda install ${WORK_DIR}/depends/executor/functools32-3.2.3.2-py27_0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/jsonschema-2.6.0-py27_0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/openssl-1.0.2k-1.tar.bz2
            conda install ${WORK_DIR}/depends/executor/pip-9.0.1-py27_1.tar.bz2
            conda install ${WORK_DIR}/depends/executor/python-2.7.13-0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/readline-6.2-2.tar.bz2
            conda install ${WORK_DIR}/depends/executor/setuptools-27.2.0-py27_0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/sqlite-3.13.0-0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/tk-8.5.18-0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/wheel-0.29.0-py27_0.tar.bz2
            conda install ${WORK_DIR}/depends/executor/zlib-1.2.8-3.tar.bz2
            conda install ${WORK_DIR}/depends/modeller/mkl-2017.0.1-0.tar.bz2
            conda install ${WORK_DIR}/depends/modeller/numpy-1.12.1-py27_0.tar.bz2

            # Special case for non-conda-package
            cp ${WORK_DIR}/depends/strict_rfc3339.py ${CONDA_DIR}/envs/kronos_environment/lib/python2.7/site-packages/

        else # install online (will download dependencies)

            echo "installing executor dependencies (online).."

            conda env create -y -n kronos_environment -f ${WORK_DIR}/kronos_executor/conda_environment_exe.txt

        fi

        echo "installing executor.."
        source activate kronos_environment
        pip install -e ${WORK_DIR}/kronos_executor

    fi

}



# === install the modeller ===
install_modeller() {

    local isoffline=$1

    # check that conda is already installed
    if [[ ! -d ${CONDA_DIR} ]]; then
        echo "Miniconda missing, install conda first.."
        exit 1
    fi

    # if modeller is already installed, return
    if [[ -d ${CONDA_DIR}/envs/kronos_environment ]]; then
        echo "kronos_environment already exists, installing modeller.."
        export PATH=${CONDA_BIN_DIR}/:${PATH}
        source activate kronos_environment
        pip install -e ${WORK_DIR}/kronos_modeller
    else

        # export conda command
        export PATH=${CONDA_BIN_DIR}/:${PATH}

        # install the conda dependencies first
        if [[ $isoffline == 1 ]]; then

            echo "installing modeller dependencies (offline).."

            # create an empty environment
            conda create -y -n kronos_environment --offline
            source activate kronos_environment

#            # Install the kronos-modeller dependencies
            conda install ${_DIRECTORY}/depends/modeller/cairo-1.14.8-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/cycler-0.10.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/dbus-1.10.10-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/expat-2.1.0-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/fontconfig-2.12.1-3.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/freetype-2.5.5-2.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/funcsigs-1.0.2-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/functools32-3.2.3.2-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/glib-2.50.2-1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/gst-plugins-base-1.8.0-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/gstreamer-1.8.0-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/icu-54.1-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/jpeg-9b-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/jsonschema-2.6.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libffi-3.2.1-1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libgcc-5.2.0-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libgfortran-3.0.0-1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libiconv-1.14-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libpng-1.6.27-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libxcb-1.12-1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/libxml2-2.9.4-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/matplotlib-2.0.1-np112py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/mkl-2017.0.1-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/mock-2.0.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/numpy-1.12.1-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/openssl-1.0.2k-1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pbr-1.10.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pcre-8.39-1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pip-9.0.1-py27_1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pixman-0.34.0-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/py-1.7.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pycairo-1.10.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pyflakes-1.5.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pyparsing-2.1.4-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pyqt-5.6.0-py27_2.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pytest-3.0.7-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/python-2.7.13-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/python-dateutil-2.6.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/pytz-2017.2-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/qt-5.6.2-3.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/readline-6.2-2.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/scikit-learn-0.18.1-np112py27_1.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/scipy-0.19.0-np112py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/setuptools-27.2.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/sip-4.18-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/six-1.10.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/sqlite-3.13.0-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/subprocess32-3.2.7-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/tk-8.5.18-0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/wheel-0.29.0-py27_0.tar.bz2
            conda install ${_DIRECTORY}/depends/modeller/zlib-1.2.8-3.tar.bz2


            # Special case for non-conda-package
            cp ${WORK_DIR}/depends/strict_rfc3339.py ${CONDA_DIR}/envs/kronos_environment/lib/python2.7/site-packages/

            # the executor is a dependency for the modeller
            pip install -e ${WORK_DIR}/kronos_executor

        else # install online (will download dependencies)

            echo "installing modeller dependencies (online).."

            conda env create -y -n kronos_environment -f ${WORK_DIR}/kronos_modeller/conda_environment_exe.txt

        fi

        echo "installing modeller.."
        source activate kronos_environment
        pip install -e ${WORK_DIR}/kronos_modeller

    fi

}





# === install the synapps ===
install_synapps() {

    echo "installing Kronos synthetic application"

    # clone ecbuild from github (master branch) - if necessary
    if [[ ! -d ${WORK_DIR}/ecbuild ]]; then
      git clone https://github.com/ecmwf/ecbuild.git || { echo "git clone failed!\n"; exit 1; }
      cd ${WORK_DIR}/ecbuild && git checkout master
    fi

    if [[ ! -d ${WORK_DIR}/build ]]; then
      mkdir ${WORK_DIR}/build
    fi

    # build the synthetic apps
    cd ${WORK_DIR}/build && cmake ${WORK_DIR} && make
}



# set an flags initial values
conda_flag=0
modeller_flag=0
executor_flag=0
synapps_flag=0
offline_flag=0
all_flag=0
help_flag=0

# define read the options
TEMP=`getopt -o cmesoah --long conda,modeller,executor,synapps,offline,all,help -- "$@"`
eval set -- "$TEMP"

# extract options and their arguments into variables.
while true ; do
    case "$1" in
        -c|--conda)
                conda_flag=1 ; shift 1 ;;
        -m|--modeller)
                modeller_flag=1 ; shift 1 ;;
        -e|--executor)
                executor_flag=1 ; shift 1 ;;
        -s|--synapps)
                synapps_flag=1 ; shift 1 ;;
        -o|--offline)
                offline_flag=1 ; shift 1 ;;
        -a|--all)
                all_flag=1 ; shift 1 ;;
        -h|--help)
                help_flag=1 ; shift 1 ;;
        --) shift ; break ;;
        *)      echo "line arguments not understood!" ; exit 1 ;;
    esac
done

echo "conda_flag: $conda_flag"
echo "modeller_flag: $modeller_flag"
echo "executor_flag: $executor_flag"
echo "synapps_flag: $synapps_flag"
echo "offline_flag: $offline_flag"
echo "help_flag: $help_flag"

# ==== check all the installation options =====
# NOTE: the offline option is passed where needed..

if [[ $all_flag == 1 ]]; then

    # install all in the correct order
    install_conda $offline_flag
    install_modeller $offline_flag
    install_executor $offline_flag
    install_synapps $offline_flag

else

    if [[ $conda_flag == 1 ]]; then
        install_conda $offline_flag
    fi

    if [[ $modeller_flag == 1 ]]; then
        install_modeller $offline_flag
    fi

    if [[ $executor_flag == 1 ]]; then
        install_executor $offline_flag
    fi

    if [[ $synapps_flag == 1 ]]; then
        install_synapps $offline_flag
    fi

    if [[ $help_flag == 1 ]]; then
        print_help
    fi

fi


