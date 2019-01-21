#!/bin/bash

set -o nounset

KRONOS_VERSION=0.6.0
KRONOS_PACKAGE=kronos-${KRONOS_VERSION}-Source
_DIRECTORY=$(dirname $(readlink -f $BASH_SOURCE))

# ====== print help documentation ======
print_help() {

  _HELP="USAGE:\n
  --modeller: install the modeller python package (with all required dependencies)\n
  --executor: install the executor python package (with all required dependencies)\n
  --syn-apps: install kronos synthetic-apps"

  echo -e ${_HELP}
}

# === download and install the conda installer ====
install_conda() {

    # download miniconda from website (exit if errors)
    set -e
    wget -c http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -P ${_DIRECTORY}
    sh ${_DIRECTORY}/Miniconda-latest-Linux-x86_64.sh -b -p ${_DIRECTORY}/miniconda
    set +e

    # update PATH
    export PATH=${_DIRECTORY}/miniconda/bin/:${PATH}
}

# === install the modeller ===
install_modeller() {

    echo "installing modeller python package.."
    pip install -e ${_DIRECTORY}/kronos_modeller
}

# === install the executor ===
install_executor() {

    echo "installing executor python package.."
    pip install -e ${_DIRECTORY}/kronos_executor
}

# === install the synapps ===
install_synapps() {

    echo "installing Kronos synthetic application"

    # clone ecbuild from github (master branch) - if necessary
    if [[ ! -d ${_DIRECTORY}/ecbuild ]]; then
      git clone https://github.com/ecmwf/ecbuild.git || { echo "git clone failed!\n"; exit 1; }
      cd ${_DIRECTORY}/ecbuild && git checkout master
    fi

    if [[ ! -d ${_DIRECTORY}/build ]]; then
      mkdir ${_DIRECTORY}/build
    fi

    # build the synthetic apps
    cd ${_DIRECTORY}/build && cmake ${_DIRECTORY} && make
}



# set an flags initial values
conda_flag=0
modeller_flag=0
executor_flag=0
synapps_flag=0
help_flag=0

# define read the options
TEMP=`getopt -o cmesh --long conda,modeller,executor,synapps,help -- "$@"`
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
        -h|--help)
                help_flag=1 ; shift 1 ;;
        --) shift ; break ;;
        *)      echo "line arguments not understood!" ; exit 1 ;;
    esac
done

#echo "conda_flag: $conda_flag"
#echo "modeller_flag: $modeller_flag"
#echo "executor_flag: $executor_flag"
#echo "synapps_flag: $synapps_flag"
#echo "help_flag: help_flag"

# ==== check all the installation options =====
if [[ $conda_flag == 1 ]]; then
    install_conda
fi

if [[ $modeller_flag == 1 ]]; then
    install_modeller
fi

if [[ $executor_flag == 1 ]]; then
    install_executor
fi

if [[ $synapps_flag == 1 ]]; then
    install_synapps
fi

if [[ $help_flag == 1 ]]; then
    print_help
fi

