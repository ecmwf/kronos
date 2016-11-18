import os
import pickle
import pprint
import subprocess
import time
import glob

import datetime

import run_control
from exceptions_iows import ConfigurationError
from ksf_handler import KSFFileHandler
from time_signal import signal_types

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from base_runner import BaseRunner
from kronos_tools import utils
from model_workload import ModelWorkload
from logreader import profiler_reader
from kronos_tools import print_colour


class FeedbackLoopRunner(BaseRunner):

    """ This class implments a Feedback loop refinement of the iows model """

    # def __init__(self, config, sa_list=None, sc_dict_init=None, reduce_flag=None):
    def __init__(self, config):

        # provide defaults if necessary
        self.config = config
        self.type = None
        self.state = None
        self.tag = None
        self.hpc_user = None
        self.hpc_host = None

        self.hpc_dir_input = None
        self.hpc_dir_output = None
        self.local_map2json_file = None

        self.n_iterations = None
        self.log_file = None

        self.synthetic_workload = None
        self.updatable_metrics = None
        self.ksf_filename = self.config.ksf_filename

        # Then set the general configuration into the parent class..
        super(FeedbackLoopRunner, self).__init__(config)

    def check_config(self):
        """
        check if the user supplied keys are consistent with this runner
        :return:
        """

        # check simple-runner configuration and pull user options..
        for k, v in self.config.runner.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected simple-runner keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def run(self):
        """
        Run the model on the HPC host according to the configuration options
        output files are left
        :return:
        None
        """

        if self.config.runner['state'] == "enabled":

            user_at_host = self.hpc_user + '@' + self.hpc_host
            time_now_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d_%H-%M-%S')
            dir_run_results = os.path.join(self.config.dir_output, 'fl_run_{}'.format(time_now_str))
            log_file = os.path.join(dir_run_results, 'log_file.txt')

            job_runner = run_control.factory(self.config.controls['hpc_job_sched'], self.config)

            # handles the ksf file
            ksf_data = KSFFileHandler().from_ksf_file(os.path.join(self.config.dir_output, self.ksf_filename))

            # create run dir
            if not os.path.exists(dir_run_results):
                os.makedirs(dir_run_results)

            metrics_sums_history = []
            scaling_factors_history = []

            # init log file
            with open(log_file, "w") as myfile:
                ts_names_str = ''.join(e + ' ' for e in signal_types.keys())
                myfile.write(ts_names_str + '\n')

            # initialize the vectors: metric_sums, tuning factors
            metrics_sum_dict_ref = ksf_data.scaled_sums
            sa_metric_dict = ksf_data.scaled_sums
            tuning_factors = ksf_data.tuning_factors

            pp = pprint.PrettyPrinter(depth=4)
            print "ts names: "
            print signal_types.keys()

            print "reference metrics: "
            print pp.pprint(metrics_sum_dict_ref)

            print "sa tot metrics: "
            print pp.pprint(sa_metric_dict)

            # write log file
            write_log_file(log_file, metrics_sum_dict_ref, tuning_factors)

            # /////////////////////////////////////// main loop.. ////////////////////////////////////////////
            for i_count in range(0, self.n_iterations):

                print "=======================================> ITERATION: ", i_count

                # create the iteration folder structure
                # -- ROOT iteration folder
                dir_run_iter = os.path.join(dir_run_results, 'iteration-{}'.format(i_count))
                if not os.path.exists(dir_run_iter):
                    os.makedirs(dir_run_iter)

                # -- SA jsons iteration folder
                dir_run_iter_sa = os.path.join(dir_run_iter, 'sa_jsons')
                if not os.path.exists(dir_run_iter_sa):
                    os.makedirs(dir_run_iter_sa)

                # -- MAP jsons iteration folder
                dir_run_iter_map = os.path.join(dir_run_iter, 'run_jsons')
                if not os.path.exists(dir_run_iter_map):
                    os.makedirs(dir_run_iter_map)

                # move the ksf file into HPC input dir (and also into SA iteration folder)
                subprocess.Popen(["scp",
                                  os.path.join(self.config.dir_output, self.ksf_filename),
                                  user_at_host+":"+self.hpc_dir_input])
                subprocess.Popen(["cp",
                                  os.path.join(self.config.dir_output, self.ksf_filename),
                                  os.path.join(dir_run_iter_sa, self.ksf_filename)])

                # -- run jobs on HPC and wait until they finish --
                job_runner.remote_run_executor()
                job_runner.have_jobs_finished()
                # ------------------------------------------------

                # -- search for ".map" files in the HPC output folder --
                sub_hdl = subprocess.Popen(["ssh", user_at_host, "find", self.hpc_dir_output, "-name", "*.map"],
                                           shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                sub_hdl.wait()

                # ------ fetch the output map files and copy them into the MAP iteration folder ------
                list_map_files = sub_hdl.stdout.readlines()
                if not list_map_files:
                    raise ValueError("No profiler files have been found after the run!")

                for (ff, file_name) in enumerate(list_map_files):
                    file_name_ok = file_name.replace("\n", "")
                    subprocess.Popen(["scp", user_at_host + ":" + file_name_ok,
                                      os.path.join(dir_run_iter_map, "job-"+str(ff)+".map")]).wait()

                    time.sleep(2.0)
                    subprocess.Popen(["python", self.local_map2json_file,
                                      os.path.join(dir_run_iter_map, "job-"+str(ff)+".map")]).wait()
                # ------------------------------------------------------------------------------------

                # ------------------ Finally rename the HPC output folder ----------------------------
                output_dst = self.hpc_dir_output.rstrip('/') + "_iter_{}".format(i_count)
                subprocess.Popen(["ssh", user_at_host, "mv", self.hpc_dir_output, output_dst]).wait()
                # ------------------------------------------------------------------------------------

                # re-calculate the workload
                time.sleep(5.0)

                # ---------- Process the run jsons ----------
                # list of run json in the run "iteration" folder
                fname_list = [file for file in os.listdir(dir_run_iter_map) if file.endswith('.json')]
                fname_list.sort()

                # job_datasets = [profiler_reader.ingest_allinea_profiles(dir_run_iter_map, jobs_n_bins, fname_list)]
                job_datasets = [profiler_reader.ingest_allinea_profiles(dir_run_iter_map, list_json_files=fname_list)]

                parsed_allinea_workload = ModelWorkload(self.config)
                parsed_allinea_workload.model_ingested_datasets(job_datasets)

                # append dictionaries to history structures..
                metrics_sum_dict = parsed_allinea_workload.total_metrics_sum_dict()
                metrics_sums_history.append(metrics_sum_dict)
                scaling_factors_history.append(tuning_factors)

                # calculate current scaling factor..
                sc_factor_dict_new = get_new_scaling_factors(metrics_sum_dict_ref,
                                                             metrics_sum_dict,
                                                             tuning_factors,
                                                             self.updatable_metrics)

                print "----------------------- summary: ---------------------------"
                print "ts names                  : ", signal_types.keys()
                print "scaling factors           : ", utils.sort_dict_list(tuning_factors, signal_types.keys())
                print "reference metrics         : ", utils.sort_dict_list(metrics_sum_dict_ref, signal_types.keys())
                print "metrics sums              : ", utils.sort_dict_list(metrics_sum_dict, signal_types.keys())
                print "scaling factors new       : ", utils.sort_dict_list(sc_factor_dict_new, signal_types.keys())
                print "scaling factors new (REL) : ", [sc_factor_dict_new[k]/tuning_factors[k] for k in signal_types.keys()]
                print "------------------------------------------------------------"

                continue_flag = raw_input("Accept these scaling factors? ")
                if (continue_flag == 'y') or (continue_flag == 'yes'):
                    tuning_factors = sc_factor_dict_new
                    pass
                else:
                    break

                # update workload through stretching and re-export the synthetic apps..
                # TODO: Note that this writes back into the output folder!! perhaps not a good choice..
                ksf_data.set_tuning_factors(tuning_factors)
                ksf_data.export(self.config.plugin['sa_n_frames'],
                                os.path.join(self.config.dir_output,self.ksf_filename))

                # write log file
                write_log_file(log_file, metrics_sum_dict, tuning_factors)
                # -----------------------------------------
            # ///////////////////////////////////// end main loop.. /////////////////////////////////////////////

        else:

            print_colour.print_colour("orange", "runner NOT enabled. Model did not run!")

    def plot_results(self):
        raise NotImplementedError("feedback loop plotter not yet implemented..")


def get_new_scaling_factors(metrics_ref, metrics_sums, sc_factors, updatable_metrics):
    """ re-calculate scaling factor according to measured deltas.. """
    met_names = updatable_metrics.keys()
    sc_factor_dict_new = {}

    for metric in met_names:
        if metrics_sums[metric] < 1.0e-10 or not updatable_metrics[metric]:
            sc_factor_dict_new[metric] = sc_factors[metric]
        else:
            sc_factor_dict_new[metric] = metrics_ref[metric] / metrics_sums[metric] * sc_factors[metric]

    return sc_factor_dict_new


def write_log_file(logfile, metrics_sum_dict, scaling_factor_dict):
    """ append metrics sums and scaling factors to log file.. """
    with open(logfile, "a") as myfile:
        metrics_str = ''.join(str(e) + ' ' for e in utils.sort_dict_list(metrics_sum_dict, signal_types.keys()))
        scaling_str = ''.join(str(e) + ' ' for e in utils.sort_dict_list(scaling_factor_dict, signal_types.keys()))
        myfile.write(metrics_str + scaling_str + '\n')

