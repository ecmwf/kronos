import os
import stat

from kronos.executor.job_classes.hpc import HPCJob


job_template = """#!/bin/sh

# ----- RAPS directives -------
#SBATCH --job-name={experiment_id}
#SBATCH --output={job_output_file}
#SBATCH --error={job_error_file}
#SBATCH -N {num_nodes}
#SBATCH --ntasks-per-node={num_procs_per_node}

#SBATCH --qos=normal
#SBATCH -p gpu
#SBATCH -J htl159
#SBATCH --cpus-per-task=1
#SBATCH --mem=128000
#SBATCH --exclusive
#SBATCH --time=01:00:00


# ----------------- dir from which RAPS jobs are to be submitted -----------------------
export RAPS_SUBMIT_DIR=/gpfs/lxg/shared_tmp/maab/RAPS17/intel.dp/flexbuild/bin/SLURM/lxg
cd $RAPS_SUBMIT_DIR

pwd
printenv | fgrep SLURM

# set up the main environment variables needed
source ../../../.again

# override out-folder to be the one dictated by Kronos
export OUTROOT="{write_dir}"


set -eux

ifsMASTER=$(which.pl ifsMASTER.*.x)

yyyymmddzz=2017073112
expver=gtky
label=LWDA

gtype=tl
resol=159
levels=137

fclen=${{fclen:-d1}} # h240 or d10

nproma=${{nproma:-16}}

nodes=$SLURM_NNODES
mpi=$SLURM_NPROCS
omp=$SLURM_CPUS_PER_TASK

ht=1

jobid=$SLURM_JOB_ID
jobname=$SLURM_JOB_NAME

host=lxg

hres \
    -p $mpi -t $omp -h $ht \
    -j $jobid -J $jobname \
    -d $yyyymmddzz -e $expver -L $label \
    -T $gtype -r $resol -l $levels -f $fclen \
    -x $ifsMASTER \
    -N $nproma \
    -H $host -n $nodes -C $compiler ${{other:-}}
"""

cancel_file_head = "#!/bin/sh\nscancel "
cancel_file_line = "{sequence_id} "


class SLURMMixin(object):
    """
    Define the templates for PBS
    """

    submit_script_template = job_template
    submit_command = "/home/ma/maab/git/kronos-core/kronos/executor/job_classes/sbatch_filter.py"
    depend_parameter = "--dependency=afterany:"
    depend_separator = ":"
    launcher_command = 'mpirun'
    allinea_launcher_command = "map --profile mpirun"

    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line


class Job(SLURMMixin, HPCJob):

    needed_config_params = [
        "num_nodes",
        "num_procs_per_node"
    ]

    def check_job_config(self):
        """
        Make sure that all the parameters needed are in the config list
        :return:
        """

        for config_param in self.needed_config_params:
            assert config_param in self.job_config["config_params"]

    def generate_internal(self):

        # update the template with the config parameters
        script_format = {
            'experiment_id': 'raps17_run_{}'.format(self.id),
            'job_num': self.id,
            'job_output_file': os.path.join(self.path, "output"),
            'job_error_file': os.path.join(self.path, "error"),
            'launcher_command': self.launcher_command,
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'input_file': self.input_file,
        }

        # update the job submit template with all the configs
        for param_name in self.needed_config_params:
            script_format.update({param_name: self.job_config["config_params"][param_name]})

        with open(self.submit_script, 'w') as f:
            f.write(self.submit_script_template.format(**script_format))

        os.chmod(self.submit_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

