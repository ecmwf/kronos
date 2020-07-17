import json
import os

from kronos_executor.hpc import HPCJob


class UserAppJob(HPCJob):

    needs_read_cache = False

    needed_config_params = None

    def __init__(self, job_config, executor, path):
        super(UserAppJob, self).__init__(job_config, executor, path)

    def customised_generated_internals(self, script_format):
        """
        User-defined generation of parts of the submit script..
        :param script_format:
        :return:
        """
        script_format.update({
            'kronos_bin_dir': os.path.dirname(self.executor.coordinator_binary)
        })

        if self.needed_config_params is not None:
            for param_name in self.needed_config_params:
                assert self.job_config["config_params"].get(param_name) is not None, param_name

        for param_name, param_val in self.job_config["config_params"].items():

            if isinstance(param_val, (int, bool, str, float)):
                script_format[param_name] = self.job_config["config_params"][param_name]

            else:  # any other composed structures are serialised as json like string

                _str = json.dumps(param_val)
                _str.replace("'", '"').replace(" ", "")
                _str = "'"+_str+"'"

                script_format[param_name] = _str

