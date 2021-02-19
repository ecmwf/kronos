#!/bin/bash

# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


# ============== DEFAULT INSTALLATION LAYOUT [*DO NOT EDIT*] ==============
export KRONOS_SOURCES_TOP_DIR=$(dirname $(readlink -f $BASH_SOURCE))
export KRONOS_VERSION=$(cat ${KRONOS_SOURCES_TOP_DIR}/VERSION.cmake | awk '{print $3}' | sed "s/\"//g")
export KRONOS_PACKAGE=kronos-${KRONOS_VERSION}-Source

# installation-related directories (default: one level up)
export KRONOS_INSTALLER_DIR_DEFAULT=${KRONOS_SOURCES_TOP_DIR}
export KRONOS_DEPENDS_DIR_DEFAULT=${KRONOS_INSTALLER_DIR_DEFAULT}/depends
export KRONOS_BUILD_DIR_DEFAULT=${KRONOS_INSTALLER_DIR_DEFAULT}/build
export KRONOS_BIN_DIR_DEFAULT=${KRONOS_BUILD_DIR_DEFAULT}/bin

# conda installation
export KRONOS_CONDA_DIR_DEFAULT=${KRONOS_INSTALLER_DIR_DEFAULT}/miniconda
export KRONOS_CONDA_BIN_DIR_DEFAULT=${KRONOS_CONDA_DIR_DEFAULT}/bin
export KRONOS_CONDA_CMD_DEFAULT=${KRONOS_CONDA_BIN_DIR_DEFAULT}/conda
export KRONOS_CONDA_INSTALLER_EXE_DEFAULT=Miniconda3-latest-Linux-x86_64.sh
# =========================================================================


# ============== (USER-OVERWRITABLE) INSTALLATION VARIABLES ===============
export KRONOS_INSTALLER_DIR=${KRONOS_INSTALLER_DIR:-$KRONOS_INSTALLER_DIR_DEFAULT}
export KRONOS_DEPENDS_DIR=${KRONOS_DEPENDS_DIR:-$KRONOS_DEPENDS_DIR_DEFAULT}
export KRONOS_BUILD_DIR=${KRONOS_BUILD_DIR:-$KRONOS_BUILD_DIR_DEFAULT}
export KRONOS_BIN_DIR=${KRONOS_BIN_DIR:-$KRONOS_BIN_DIR_DEFAULT}
export KRONOS_CONDA_DIR=${KRONOS_CONDA_DIR:-$KRONOS_CONDA_DIR_DEFAULT}
export KRONOS_CONDA_BIN_DIR=${KRONOS_CONDA_BIN_DIR:-$KRONOS_CONDA_BIN_DIR_DEFAULT}
export KRONOS_CONDA_CMD=${KRONOS_CONDA_CMD:-$KRONOS_CONDA_CMD_DEFAULT}
export KRONOS_CONDA_INSTALLER_EXE=${KRONOS_CONDA_INSTALLER_EXE:-$KRONOS_CONDA_INSTALLER_EXE_DEFAULT}
# =========================================================================


# log kronos env to stdout
env 2>&1 | grep KRONOS | grep -v DEFAULT



# ====== print help documentation ======
print_help() {
  echo -e "\nUSAGE:\n"
  echo -e "--modeller : install the modeller python package (with all required dependencies)"
  echo -e "--executor : install the executor python package (with all required dependencies)"
  echo -e "--synapps  : install kronos synthetic-apps"
  echo -e "--offline  : if available, use packaged deps for conda and/or kronos environments"
  echo -e "--all      : install conda, kronos-executor, kronos-modeller, synthetic apps"
  echo -e "--help     : show this help"
  echo -e "\nEXAMPLES:\n"
  echo -e "  1. install conda and the kronos-executor environment (offline)"
  echo -e "    > ./install.sh --conda --executor --offline (or ./install -ceo)\n"
  echo -e "  2. install conda and the kronos-modeller environment (online)"
  echo -e "    > ./install.sh --conda --modeller --offline (or ./install -cmo)\n"
  echo -e "  3. install conda, kronos-executor, kronos-modeller and the synthetic apps (offline)"
  echo -e "    > ./install.sh --all --offline (or ./install -ao)\n"
}

# === download and install the conda installer ====
install_conda() {

    local isoffline=$1

    if [[ -d ${KRONOS_CONDA_DIR} ]]; then
        echo "conda seems already installed in ${KRONOS_CONDA_DIR}, skipping installation.."
        return 0
    fi

    if [[ $isoffline == 1 ]]; then

        # install offline (it require the depends directory with all the dependencies in it)
        local conda_inst_offline=${KRONOS_DEPENDS_DIR}/${KRONOS_CONDA_INSTALLER_EXE}
        if [[ ! -f ${conda_inst_offline} ]]; then
            echo "Asked to install conda offline but ${conda_inst_offline} not found!"
            exit 1
        else
            set -e
            echo "Found conda in /depends: installing.."
            sh ${conda_inst_offline} -b -p ${KRONOS_CONDA_DIR}
            set +e
        fi

    else
        # download miniconda from website (exit if errors)
        set -e
        wget -c http://repo.continuum.io/miniconda/${KRONOS_CONDA_INSTALLER_EXE} -P ${KRONOS_INSTALLER_DIR}
        sh ${KRONOS_INSTALLER_DIR}/${KRONOS_CONDA_INSTALLER_EXE} -b -p ${KRONOS_CONDA_DIR}
        set +e
    fi

}


# === install the executor ===
install_executor() {

    local isoffline=$1

    # check that conda is already installed
    if [[ ! -d ${KRONOS_CONDA_DIR} ]]; then
        echo "Miniconda missing, install conda first.."
        exit 1
    fi

    # if executor is already installed, return
    if [[ -d ${KRONOS_CONDA_DIR}/envs/kronos_executor_env ]]; then
        echo "kronos_executor_env already exists, installing executor.."
        export PATH=${KRONOS_CONDA_BIN_DIR}/:${PATH}
        source activate kronos_executor_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_executor
        conda deactivate
    else

        # export conda command
        export PATH=${KRONOS_CONDA_BIN_DIR}/:${PATH}

        set -e

        # install the conda dependencies first
        if [[ $isoffline == 1 ]]; then

            if [[ ! -f ${KRONOS_DEPENDS_DIR}/kronos_executor.deps ]] ; then
                echo "this installer does not ship the dependencies for an offline installation"
                exit 1
            fi

            echo "installing executor dependencies (offline).."

            # create an empty environment
            conda create -y -n kronos_executor_env --offline
            source activate kronos_executor_env

            # Install the kronos-executor dependencies
            while read depfile ; do
                if echo $depfile | grep -qv '^pip:' ; then
                    echo "Installing $depfile"
                    conda install ${KRONOS_DEPENDS_DIR}/$depfile
                else
                    depfile=${depfile#pip:}
                    echo "Installing $depfile (pip)"
                    pip install ${KRONOS_DEPENDS_DIR}/$depfile
                fi
            done < ${KRONOS_DEPENDS_DIR}/kronos_executor.deps

            conda deactivate

        else # install online (will download dependencies)

            echo "installing executor dependencies (online).."

            conda env create -n kronos_executor_env -f ${KRONOS_SOURCES_TOP_DIR}/kronos_executor/conda_env_executor.yml

        fi

        echo "installing executor.."
        source activate kronos_executor_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_executor
        conda deactivate

        set +e

    fi

}



# === install the modeller ===
install_modeller() {

    local isoffline=$1

    # check that conda is already installed
    if [[ ! -d ${KRONOS_CONDA_DIR} ]]; then
        echo "Miniconda missing, install conda first.."
        exit 1
    fi

    # if modeller is already installed, return
    if [[ -d ${KRONOS_CONDA_DIR}/envs/kronos_modeller_env ]]; then
        echo "kronos_modeller_env already exists, installing modeller.."
        export PATH=${KRONOS_CONDA_BIN_DIR}/:${PATH}
        source activate kronos_modeller_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_modeller
        conda deactivate
    else

        # export conda command
        export PATH=${KRONOS_CONDA_BIN_DIR}/:${PATH}

        set -e

        # install the conda dependencies first
        if [[ $isoffline == 1 ]]; then

            if [[ ! -f ${KRONOS_DEPENDS_DIR}/kronos_modeller.deps ]] ; then
                echo "this installer does not ship the dependencies for an offline installation"
                exit 1
            fi

            echo "installing modeller dependencies (offline).."

            # create an empty environment
            conda create -y -n kronos_modeller_env --offline
            source activate kronos_modeller_env

            # Install the kronos-modeller dependencies
            while read depfile ; do
                if echo $depfile | grep -qv '^pip:' ; then
                    echo "Installing $depfile"
                    conda install ${KRONOS_DEPENDS_DIR}/$depfile
                else
                    depfile=${depfile#pip:}
                    echo "Installing $depfile (pip)"
                    pip install ${KRONOS_DEPENDS_DIR}/$depfile
                fi
            done < ${KRONOS_DEPENDS_DIR}/kronos_modeller.deps

            conda deactivate

        else # install online (will download dependencies)

            echo "installing modeller dependencies (online).."

            conda env create -n kronos_modeller_env -f ${KRONOS_SOURCES_TOP_DIR}/kronos_modeller/conda_env_modeller.yml

        fi

        echo "installing modeller.."
        source activate kronos_modeller_env

        # the executor is a dependency for the modeller
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_executor

        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_modeller

        conda deactivate

        set +e

    fi

}


# === install the synapps ===
install_synapps() {

    local isoffline=$1

    if [[ $isoffline == 1 ]]; then

        echo "installing Kronos synthetic application (offline)"
        echo "this needs that ecbuild files already exist in ${KRONOS_SOURCES_TOP_DIR}/cmake"

    else

        echo "installing Kronos synthetic application (online)"
        echo "this will download ecbuild.."

        # clone ecbuild from github (master branch)
        if [[ ! -d ${KRONOS_SOURCES_TOP_DIR}/../ecbuild ]]; then

          ECBUILD_DIR=$(readlink -f "${KRONOS_SOURCES_TOP_DIR}/../ecbuild")

          echo -e "ecbuild not found, downloading in ${ECBUILD_DIR}. Proceed? [y/n]"
          read user_ans
          echo -e "\n"

          if [[ $user_ans =~ ^[Yy]$ ]]; then
            git clone https://github.com/ecmwf/ecbuild.git ${ECBUILD_DIR} || { echo "git clone failed!\n"; exit 1; }
            cd ${ECBUILD_DIR} && git checkout master
          else
            echo "not downloading, stopping here!"
            exit 1
          fi
        fi

    fi

    if [[ ! -d ${KRONOS_BUILD_DIR} ]]; then
      mkdir ${KRONOS_BUILD_DIR}
    fi

    # export python command
    export PATH=${KRONOS_CONDA_BIN_DIR}/:${PATH}

    # build the synthetic apps
    cd ${KRONOS_BUILD_DIR} && cmake ${KRONOS_SOURCES_TOP_DIR} && make
}


# /////////////////////////////////// MAIN ///////////////////////////////////

echo -e "\n **** install script of Kronos project ****\n"
echo -e "KRONOS_VERSION: ${KRONOS_VERSION}"
echo -e "KRONOS_PACKAGE: ${KRONOS_PACKAGE}"

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

if [[ $# < 2 ]]; then
    print_help
    exit 1
fi


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
        *)
        echo "line arguments not valid." ;
        print_help
        exit 1 ;;
    esac
done


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


