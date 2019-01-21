
# current working directory
WORK_DIR=$(dirname $(readlink -f $BASH_SOURCE))

# export path and activate the kronos environment
export PATH=${WORK_DIR}/miniconda/bin/:${PATH}
source activate kronos_modeller_env

