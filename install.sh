#!/bin/bash

KRONOS_VERSION=0.6.0
KRONOS_PACKAGE=kronos-${KRONOS_VERSION}-Source

_DIRECTORY=$(dirname $(readlink -f $BASH_SOURCE))

_HELP="USAGE:\n
--modeller: install the modeller python package (with all required dependencies)\n
--executor: install the executor python package (with all required dependencies)\n
--syn-apps: install kronos synthetic-apps
"
  

echo "BASE directory: ${_DIRECTORY}"

cd ${_DIRECTORY}

if [[ $# != 1 ]]; then

  echo -e "Wrong parameters passed!"
  echo -e ${_HELP}
  exit 1

else

  if [[ $1 == "--modeller" ]]; then

    echo "installing modeller python package.."
    pip install -e ${_DIRECTORY}/kronos_modeller

  elif [[ $1 == "--executor" ]]; then

    echo "installing executor python package.."
    pip install -e ${_DIRECTORY}/kronos_executor
   
  elif [[ $1 == "--syn-apps" ]]; then

    echo "installing Kronos synthetic application"

    # clone ecbuild from github (master branch)
    git clone https://github.com/ecmwf/ecbuild.git
    cd ${_DIRECTORY}/ecbuild && git checkout master
    if [[ ! -d ${_DIRECTORY}/build ]]; then
      mkdir ${_DIRECTORY}/build 
    fi

    # build the synthetic apps
    cd ${_DIRECTORY}/build && cmake ${_DIRECTORY} && make

  else
    echo -e "Wrong parameters passed!"
    echo -e ${_HELP}
    exit 1
  fi 

fi

