import json
import os

from kronos_executor.hpc import HPCJob


class UserAppJob(HPCJob):

    needs_read_cache = False

    needed_config_params = None

    def __init__(self, job_config, executor, path):
        super(UserAppJob, self).__init__(job_config, executor, path)

    def check_job_config(self):
        """
        Make sure that all the parameters needed are in the config list
        :return:
        """

        if not self.needed_config_params:
            raise KeyError("No parameters to be passed to the job submit "
                           "template have been defined!")

        for config_param in self.needed_config_params:
            assert config_param in self.job_config["config_params"]

    def customised_generated_internals(self, script_format):
        """
        User-defined generation of parts of the submit script..
        :param script_format:
        :return:
        """
        script_format.update({
            'kronos_bin_dir': os.path.dirname(self.executor.coordinator_binary)
        })

        assert self.needed_config_params is not None
        for param_name in self.needed_config_params:
            assert self.job_config["config_params"].get(param_name) is not None, param_name

        for param_name in self.job_config["config_params"].keys():

            if isinstance(self.job_config["config_params"][param_name], int) or \
                isinstance(self.job_config["config_params"][param_name], bool) or \
                  isinstance(self.job_config["config_params"][param_name], str) or \
                    isinstance(self.job_config["config_params"][param_name], float):
                        script_format.update({param_name: self.job_config["config_params"][param_name]})

            else:  # any other composed structures are serialised as json like string

                _str = json.dumps(self.job_config["config_params"][param_name])
                _str.replace("'", '"').replace(" ", "")
                _str = "'"+_str+"'"

                script_format.update({param_name: _str})

