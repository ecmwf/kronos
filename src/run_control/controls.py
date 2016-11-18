# from run_control.base_controls import BASEControls
import re
import subprocess
import time
import os

from exceptions_iows import ConfigurationError
from config.config import Config
from kronos_tools import print_colour


class BASEControls(object):

    def __init__(self, config):
        assert isinstance(config, Config)
        self.config = config
        self.check_config()

    def check_config(self):
        """
        checks and sets default config options
        """
        pass

    def remote_run_executor(self):
        raise NotImplementedError("Must be implemented in derived class..")

    def have_jobs_finished(self):
        raise NotImplementedError("Must be implemented in derived class..")


class PBSControls(BASEControls):
    """
    implements some commands to control remote jobs with PBS scheduler
    """

    def __init__(self, config):

        # controller-specific configuration needed
        self.hpc_user = None
        self.hpc_host = None
        self.hpc_job_sched = None
        self.hpc_shell_type = None
        self.hpc_module_dir_init = None
        self.hpc_module_env = None
        self.hpc_dir_exec = None
        self.hpc_dir_input = None
        self.hpc_dir_output = None
        self.hpc_exec_config = None
        self.local_map2json_file = None

        # Then set the general configuration into the parent class..
        super(PBSControls, self).__init__(config)

    def check_config(self):
        """
        Update the default values using the supplied configuration dict
        :return:
        """
        for k, v in self.config.controls.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected PBSControls keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def remote_run_executor(self):
        """
        Run the executor on the host
        :return:
        """

        user_at_host = self.hpc_user+'@'+self.hpc_host

        # source module init if necessary..
        mod_load_init = ''
        if self.hpc_module_dir_init:
            mod_load_init = 'source ' + os.path.join(self.hpc_module_dir_init, self.hpc_shell_type) + " && "

        mod_load_str = ''
        for mod in self.hpc_module_env:
            mod_load_str = mod_load_init + "module load " + mod + " && "

        executor_str = os.path.join(self.hpc_dir_exec, "executor.py") + ' ' + self.hpc_exec_config
        subprocess.Popen(["ssh", user_at_host, mod_load_str + executor_str]).wait()
        time.sleep(2.0)

    def have_jobs_finished(self):
        """
        check if all the jobs have finished running on host
        :return:
        """

        jobs_completed = False
        user_at_host = self.hpc_user + '@' + self.hpc_host

        while not jobs_completed:
            ssh_ls_cmd = subprocess.Popen(["ssh", user_at_host, 'qstat -u', self.hpc_user],
                                          shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            jobs_in_queue = ssh_ls_cmd.stdout.readlines()

            if not jobs_in_queue:
                jobs_completed = True
                print_colour.print_colour("green", "jobs completed!")

            time.sleep(2.0)


class SLURMControls(BASEControls):
    """
    implements some commands to control remote jobs with PBS scheduler
    """

    def __init__(self, config):

        # controller-specific configuration needed
        self.hpc_user = None
        self.hpc_host = None
        self.hpc_job_sched = None
        self.hpc_shell_type = None
        self.hpc_module_dir_init = None
        self.hpc_module_env = None

        self.hpc_dir_exec = None
        self.hpc_exec_config = None

        # Then set the general configuration into the parent class..
        super(SLURMControls, self).__init__(config)

    def check_config(self):
        """
        Update the default values using the supplied configuration dict
        :return:
        """
        for k, v in self.config.controls.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected SLURMControls keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def remote_run_executor(self):
        """
        Run the executor on the host
        :return:
        """

        user_at_host = self.hpc_user+'@'+self.hpc_host

        mod_load_str = 'source ' + os.path.join(self.hpc_module_dir_init, self.hpc_shell_type)
        for mod in self.hpc_module_env:
            mod_load_str = mod_load_str + " && module load " + mod

        executor_str = os.path.join(self.hpc_dir_exec, "executor.py") + ' ' + self.hpc_exec_config
        subprocess.Popen(["ssh", user_at_host, mod_load_str + " && " + executor_str]).wait()
        time.sleep(2.0)

    def have_jobs_finished(self):
        """
        check if all the jobs have finished running on host
        :return:
        """

        jobs_completed = False
        user_at_host = self.hpc_user + '@' + self.hpc_host

        while not jobs_completed:
            ssh_ls_cmd = subprocess.Popen(["ssh",
                                           user_at_host,
                                           '/usr/local/apps/slurm/16.05.4/bin/squeue -u',
                                           self.hpc_user],
                                          shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            jobs_in_queue = ssh_ls_cmd.stdout.readlines()

            # str_sl = '             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)'
            # TODO: write this check properly..
            if len(jobs_in_queue) == 1:
                jobs_completed = True
                print_colour.print_colour("green", "jobs completed!")

            time.sleep(2.0)


class LocalControls(BASEControls):
    """
    implements some commands to control local jobs
    """

    def __init__(self, config):

        # controller-specific configuration needed
        self.hpc_user = None
        self.hpc_host = None
        self.hpc_job_sched = None
        self.hpc_shell_type = None
        self.hpc_module_dir_init = None
        self.hpc_module_env = None

        self.hpc_dir_exec = None
        self.hpc_exec_config = None

        # Then set the general configuration into the parent class..
        super(LocalControls, self).__init__(config)

    def check_config(self):
        """
        Update the default values using the supplied configuration dict
        :return:
        """
        for k, v in self.config.controls.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected LocalControls keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def remote_run_executor(self):
        """
        Run the executor on the host
        :return:
        """

        mod_load_str = 'source ' + os.path.join(self.hpc_module_dir_init, self.hpc_shell_type)
        for mod in self.hpc_module_env:
            mod_load_str = mod_load_str + " && module load " + mod

        executor_str = os.path.join(self.hpc_dir_exec, "executor.py") + ' ' + self.hpc_exec_config
        # subprocess.Popen([mod_load_str + " && " + executor_str]).wait()
        print executor_str
        subprocess.Popen([executor_str], shell=True).wait()
        print executor_str

    def have_jobs_finished(self):
        """
        check if all the jobs have finished running on host
        :return:
        """

        jobs_completed = False
        print "checking for job completion.."

        while not jobs_completed:
            ssh_ls_cmd = subprocess.Popen(['ps -eaf | grep', 'coordinator'],
                                          shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            jobs_in_queue = ssh_ls_cmd.stdout.read()

            if re.search(self.hpc_dir_exec+'coordinator', jobs_in_queue) is None:
                jobs_completed = True
                print_colour.print_colour("green", "jobs completed!")

            time.sleep(2.0)
