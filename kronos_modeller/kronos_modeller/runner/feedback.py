# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import datetime
import json
import time
import pprint
import logging
import subprocess

from base_runner import BaseRunner
from kronos_executor.io_formats.schedule_format import ScheduleFormat
from kronos_modeller import run_control
from kronos_modeller import time_signal
from kronos_executor.definitions import signal_types
from kronos_modeller.kronos_exceptions import ConfigurationError
from kronos_modeller.kronos_tools import utils
from kronos_modeller.logreader import profiler_reader
from kronos_modeller.workload_data import WorkloadData

logger = logging.getLogger(__name__)


class FeedbackLoopRunner(BaseRunner):

    """ This class implments a Feedback loop refinement of the iows model """

    required_fields = [
        'type',
        'state',
        'n_iterations',
        'hpc_user',
        'hpc_host',
        'tag',
        'hpc_dir_input',
        'hpc_dir_output',
        'local_map2json_file',
        'updatable_metrics',
    ]

    # def __init__(self, config, sa_list=None, sc_dict_init=None, reduce_flag=None):
    def __init__(self, config):

        # provide defaults if necessary
        self.config = config

        self.type = None
        self.state = None
        self.n_iterations = None

        self.hpc_user = None
        self.hpc_host = None
        self.tag = None

        self.hpc_dir_input = None
        self.hpc_dir_output = None
        self.local_map2json_file = None

        self.updatable_metrics = None
        # self.controls = None

        # self.log_file = None
        self.synthetic_workload = None
        self.kschedule_filename = self.config.kschedule_filename

        # Then set the general configuration into the parent class..
        super(FeedbackLoopRunner, self).__init__(config)

    def check_config(self):
        """
        Check that all the required fields are passed correctly
        :return:
        """

        for req_item in self.required_fields:
            if req_item not in self.config.run.keys():
                raise ConfigurationError("{} requires to specify {}".format(self.__class__.__name__, req_item))
            setattr(self, req_item, self.config.run[req_item])

    def run(self):
        """
        Run the model on the HPC host according to the configuration options
        output files are left
        :return:
        None
        """

        if self.config.run['state'] == "enabled":

            user_at_host = self.hpc_user + '@' + self.hpc_host
            time_now_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d_%H-%M-%S')
            dir_run_results = os.path.join(self.config.dir_output,
                                           self.tag+'_run_{}'.format(time_now_str))
            log_file = os.path.join(dir_run_results, 'log_file.txt')

            job_runner = run_control.factory(self.config.run['hpc_job_sched'], self.config)

            # handles the kschedule file
            kschedule_data = ScheduleFormat.from_filename(os.path.join(self.config.dir_output, self.kschedule_filename))

            # create run dir
            if not os.path.exists(dir_run_results):
                os.makedirs(dir_run_results)

            metrics_sums_history = []
            scaling_factors_history = []

            # init log file
            with open(log_file, "w") as myfile:
                ts_names_str = ''.join(e + ' ' for e in signal_types.keys())
                myfile.write(ts_names_str + '\n')

            # initialize the vectors: metric_sums, scaling factors
            metrics_sum_dict_ref = kschedule_data.scaled_sums
            sa_metric_dict = kschedule_data.scaled_sums
            scaling_factors = kschedule_data.scaling_factors

            pp = pprint.PrettyPrinter(depth=4)
            print "ts names: "
            print time_signal.time_signal_names

            print "reference metrics: "
            print pp.pprint(metrics_sum_dict_ref)

            print "sa tot metrics: "
            print pp.pprint(sa_metric_dict)

            # write log file
            write_log_file(log_file, metrics_sum_dict_ref, scaling_factors)

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

                # move the kschedule file into HPC input dir (and also into SA iteration folder)
                subprocess.Popen(["scp",
                                  os.path.join(self.config.dir_output, self.kschedule_filename),
                                  user_at_host+":"+self.hpc_dir_input]).wait()
                subprocess.Popen(["cp",
                                  os.path.join(self.config.dir_output, self.kschedule_filename),
                                  os.path.join(dir_run_iter_sa, self.kschedule_filename)]).wait()

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

                for file_name in list_map_files:
                    file_name_ok = file_name.replace("\n", "")
                    file_id = file_name_ok.split('/')[-2]

                    subprocess.Popen(["scp", user_at_host + ":" + file_name_ok,
                                      os.path.join(dir_run_iter_map, file_id+".map")]).wait()

                    subprocess.Popen(["scp", user_at_host + ":" + os.path.join(os.path.dirname(file_name_ok), 'input.json'),
                                      os.path.join(dir_run_iter_map, file_id+"_input.json")]).wait()

                    time.sleep(2.0)
                    subprocess.Popen(["python", self.local_map2json_file,
                                      os.path.join(dir_run_iter_map, file_id+".map")]).wait()
                # ------------------------------------------------------------------------------------

                # ------------------ Finally rename the HPC output folder ----------------------------
                output_dst = self.hpc_dir_output.rstrip('/') + "_iter_{}".format(i_count)
                subprocess.Popen(["ssh", user_at_host, "mv", self.hpc_dir_output, output_dst]).wait()
                # ------------------------------------------------------------------------------------

                # re-calculate the workload
                time.sleep(5.0)

                # ---------- Process the run jsons ----------
                # list of run json in the run "iteration" folder (and dictionary file->workload_label)
                fname_list = []
                dict_name_label = {}
                for file_name in os.listdir(dir_run_iter_map):
                    if file_name.endswith('.json') and "_input" not in file_name:
                        fname_list.append(file_name)

                        print "file_name", file_name

                        # read the corresponding json of the input and read the label
                        with open(os.path.join(dir_run_iter_map, file_name.split('.')[0]+'_input.json'), 'r') as f:
                            json_data = json.load(f)

                        label = json_data['metadata']['workload_name']
                        dict_name_label[file_name] = label

                print 'dict_name_label', dict_name_label

                fname_list.sort()
                job_map_dataset = profiler_reader.ingest_allinea_profiles(dir_run_iter_map,
                                                                          list_json_files=fname_list,
                                                                          json_label_map=dict_name_label)

                # the data from the datasets are loaded into a list of model jobs
                map_workload = WorkloadData(jobs=[job for job in job_map_dataset.model_jobs()],
                                            tag='allinea_map_files')

                # export the workload to a kprofile file
                ScheduleFormat.from_synthetic_workload(map_workload).write_filename(os.path.join(dir_run_iter_map, 'kprofile_output.kprofile'))
                # ///////////////////////////////////////////////////////////////////////////////////////

                # append dictionaries to history structures..
                metrics_sum_dict = map_workload.total_metrics_sum_dict
                metrics_sums_history.append(metrics_sum_dict)
                scaling_factors_history.append(scaling_factors)

                # calculate current scaling factor..
                sc_factor_dict_new = get_new_scaling_factors(metrics_sum_dict_ref,
                                                             metrics_sum_dict,
                                                             scaling_factors,
                                                             self.updatable_metrics)

                print "----------------------- summary: ---------------------------"
                print "ts names                  : ", time_signal.time_signal_names
                print "scaling factors           : ", utils.sort_dict_list(scaling_factors, time_signal.time_signal_names)
                print "reference metrics         : ", utils.sort_dict_list(metrics_sum_dict_ref, time_signal.time_signal_names)
                print "metrics sums              : ", utils.sort_dict_list(metrics_sum_dict, time_signal.time_signal_names)
                print "scaling factors new       : ", utils.sort_dict_list(sc_factor_dict_new, time_signal.time_signal_names)
                print "scaling factors new (REL) : ", [sc_factor_dict_new[k]/scaling_factors[k] for k in time_signal.time_signal_names]
                print "------------------------------------------------------------"

                continue_flag = raw_input("Accept these scaling factors? ")
                if (continue_flag == 'y') or (continue_flag == 'yes'):
                    scaling_factors = sc_factor_dict_new
                    pass
                else:
                    break

                # update workload through stretching and re-export the synthetic apps..
                # TODO: Note that this writes back into the output folder!! perhaps not a good choice..
                kschedule_data.set_scaling_factors(scaling_factors)

                print os.path.join(self.config.dir_output, self.kschedule_filename)
                kschedule_data.export(filename=os.path.join(self.config.dir_output, self.kschedule_filename),
                                nbins=self.config.model['generator']['synthapp_n_frames'])

                # write log file
                write_log_file(log_file, metrics_sum_dict, scaling_factors)
                # -----------------------------------------
            # ///////////////////////////////////// end main loop.. /////////////////////////////////////////////

        else:

            logger.info( "runner NOT enabled. Model did not run!")

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
        metrics_str = ''.join(str(e) + ' ' for e in utils.sort_dict_list(metrics_sum_dict, time_signal.time_signal_names))
        scaling_str = ''.join(str(e) + ' ' for e in utils.sort_dict_list(scaling_factor_dict, time_signal.time_signal_names))
        myfile.write(metrics_str + scaling_str + '\n')

