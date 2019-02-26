#!/bin/bash

set -o nounset

# working dir
KRONOS_SOURCES_TOP_DIR=$(dirname $(readlink -f $BASH_SOURCE))

# kronos version/package
KRONOS_VERSION=$(cat ${KRONOS_SOURCES_TOP_DIR}/VERSION.cmake | awk '{print $3}' | sed "s/\"//g")
KRONOS_PACKAGE=kronos-${KRONOS_VERSION}-Source


# installation-related directories (all one level above..)
INSTALLER_DIR=$(dirname ${KRONOS_SOURCES_TOP_DIR})
DEPENDS_DIR=${INSTALLER_DIR}/depends
CONDA_DIR=${INSTALLER_DIR}/miniconda
CONDA_BIN_DIR=${CONDA_DIR}/bin
CONDA_CMD=${CONDA_BIN_DIR}/conda
CONDA_INSTALLER_EXE=Miniconda2-latest-Linux-x86_64.sh

KRONOS_BUILD_DIR=${INSTALLER_DIR}/build

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

    if [[ -d ${CONDA_DIR} ]]; then
        echo "conda seems already installed in ${CONDA_DIR}, skipping installation.."
        return 0
    fi

    if [[ $isoffline == 1 ]]; then

        # install offline (it require the depends directory with all the dependencies in it)
        local conda_inst_offline=${DEPENDS_DIR}/${CONDA_INSTALLER_EXE}
        if [[ ! -f ${conda_inst_offline} ]]; then
            echo "Asked to install conda offline but ${conda_inst_offline} not found!"
            exit 1
        else
            set -e
            echo "Found conda in /depends: installing.."
            sh ${conda_inst_offline} -b -p ${CONDA_DIR}
            set +e
        fi

    else
        # download miniconda from website (exit if errors)
        set -e
        wget -c http://repo.continuum.io/miniconda/${CONDA_INSTALLER_EXE} -P ${CONDA_DIR}
        sh ${CONDA_DIR}/${CONDA_INSTALLER_EXE} -b -p ${CONDA_DIR}
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
    if [[ -d ${CONDA_DIR}/envs/kronos_executor_env ]]; then
        echo "kronos_executor_env already exists, installing executor.."
        export PATH=${CONDA_BIN_DIR}/:${PATH}
        source activate kronos_executor_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_executor
    else

        # export conda command
        export PATH=${CONDA_BIN_DIR}/:${PATH}

        # install the conda dependencies first
        if [[ $isoffline == 1 ]]; then

            echo "installing executor dependencies (offline).."

            # create an empty environment
            conda create -y -n kronos_executor_env --offline
            source activate kronos_executor_env

            # Install the kronos-executor dependencies
            conda install ${DEPENDS_DIR}/functools32-3.2.3.2-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/jsonschema-2.6.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/openssl-1.0.2k-1.tar.bz2
            conda install ${DEPENDS_DIR}/pip-9.0.1-py27_1.tar.bz2
            conda install ${DEPENDS_DIR}/python-2.7.13-0.tar.bz2
            conda install ${DEPENDS_DIR}/readline-6.2-2.tar.bz2
            conda install ${DEPENDS_DIR}/setuptools-27.2.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/sqlite-3.13.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/tk-8.5.18-0.tar.bz2
            conda install ${DEPENDS_DIR}/wheel-0.29.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/zlib-1.2.8-3.tar.bz2
            conda install ${DEPENDS_DIR}/mkl-2017.0.1-0.tar.bz2
            conda install ${DEPENDS_DIR}/numpy-1.12.1-py27_0.tar.bz2

            # Special case for non-conda-package
            cp ${DEPENDS_DIR}/strict_rfc3339.py ${CONDA_DIR}/envs/kronos_executor_env/lib/python2.7/site-packages/

        else # install online (will download dependencies)

            echo "installing executor dependencies (online).."

            conda env create -n kronos_executor_env -f ${KRONOS_SOURCES_TOP_DIR}/kronos_executor/conda_environment_exe.txt

        fi

        echo "installing executor.."
        source activate kronos_executor_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_executor

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
    if [[ -d ${CONDA_DIR}/envs/kronos_modeller_env ]]; then
        echo "kronos_modeller_env already exists, installing modeller.."
        export PATH=${CONDA_BIN_DIR}/:${PATH}
        source activate kronos_modeller_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_modeller
    else

        # export conda command
        export PATH=${CONDA_BIN_DIR}/:${PATH}

        # install the conda dependencies first
        if [[ $isoffline == 1 ]]; then

            echo "installing modeller dependencies (offline).."

            # create an empty environment
            conda create -y -n kronos_modeller_env --offline
            source activate kronos_modeller_env

            # Install the kronos-modeller dependencies
            conda install ${DEPENDS_DIR}/cairo-1.14.8-0.tar.bz2
            conda install ${DEPENDS_DIR}/cycler-0.10.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/dbus-1.10.10-0.tar.bz2
            conda install ${DEPENDS_DIR}/expat-2.1.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/fontconfig-2.12.1-3.tar.bz2
            conda install ${DEPENDS_DIR}/freetype-2.5.5-2.tar.bz2
            conda install ${DEPENDS_DIR}/funcsigs-1.0.2-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/functools32-3.2.3.2-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/glib-2.50.2-1.tar.bz2
            conda install ${DEPENDS_DIR}/gst-plugins-base-1.8.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/gstreamer-1.8.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/icu-54.1-0.tar.bz2
            conda install ${DEPENDS_DIR}/jpeg-9b-0.tar.bz2
            conda install ${DEPENDS_DIR}/jsonschema-2.6.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/libffi-3.2.1-1.tar.bz2
            conda install ${DEPENDS_DIR}/libgcc-5.2.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/libgfortran-3.0.0-1.tar.bz2
            conda install ${DEPENDS_DIR}/libiconv-1.14-0.tar.bz2
            conda install ${DEPENDS_DIR}/libpng-1.6.27-0.tar.bz2
            conda install ${DEPENDS_DIR}/libxcb-1.12-1.tar.bz2
            conda install ${DEPENDS_DIR}/libxml2-2.9.4-0.tar.bz2
            conda install ${DEPENDS_DIR}/matplotlib-2.0.1-np112py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/mkl-2017.0.1-0.tar.bz2
            conda install ${DEPENDS_DIR}/mock-2.0.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/numpy-1.12.1-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/openssl-1.0.2k-1.tar.bz2
            conda install ${DEPENDS_DIR}/pbr-1.10.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/pcre-8.39-1.tar.bz2
            conda install ${DEPENDS_DIR}/pip-9.0.1-py27_1.tar.bz2
            conda install ${DEPENDS_DIR}/pixman-0.34.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/py-1.7.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/pycairo-1.10.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/pyflakes-1.5.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/pyparsing-2.1.4-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/pyqt-5.6.0-py27_2.tar.bz2
            conda install ${DEPENDS_DIR}/pytest-3.0.7-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/python-2.7.13-0.tar.bz2
            conda install ${DEPENDS_DIR}/python-dateutil-2.6.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/pytz-2017.2-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/qt-5.6.2-3.tar.bz2
            conda install ${DEPENDS_DIR}/readline-6.2-2.tar.bz2
            conda install ${DEPENDS_DIR}/scikit-learn-0.18.1-np112py27_1.tar.bz2
            conda install ${DEPENDS_DIR}/scipy-0.19.0-np112py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/setuptools-27.2.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/sip-4.18-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/six-1.10.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/sqlite-3.13.0-0.tar.bz2
            conda install ${DEPENDS_DIR}/subprocess32-3.2.7-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/tk-8.5.18-0.tar.bz2
            conda install ${DEPENDS_DIR}/wheel-0.29.0-py27_0.tar.bz2
            conda install ${DEPENDS_DIR}/zlib-1.2.8-3.tar.bz2


            # Special case for non-conda-package
            cp ${DEPENDS_DIR}/strict_rfc3339.py ${CONDA_DIR}/envs/kronos_modeller_env/lib/python2.7/site-packages/

            # the executor is a dependency for the modeller
            pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_executor

        else # install online (will download dependencies)

            echo "installing modeller dependencies (online).."

            conda env create -n kronos_modeller_env -f ${KRONOS_SOURCES_TOP_DIR}/kronos_modeller/conda_environment.txt

        fi

        echo "installing modeller.."
        source activate kronos_modeller_env
        pip install -e ${KRONOS_SOURCES_TOP_DIR}/kronos_modeller

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


