import os
import subprocess
import time
import glob

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from kronos_tools import utils
from model_workload import ModelWorkload
from synthetic_app import SyntheticWorkload
from logreader import profiler_reader


class FeedbackLoop(object):

    """ This class implments a Feedback loop refinement of the iows model """

    def __init__(self, config, sa_list=None, sc_dict_init=None, reduce_flag=None):

        self.config = config
        self.sa_list = sa_list
        self.sc_dict_init = sc_dict_init
        self.reduce_flag = reduce_flag

        # it makes a workload from ssa list..
        self.synthetic_workload = SyntheticWorkload(self.config, apps=self.sa_list)
        self.synthetic_workload.export(self.config.IOWSMODEL_TOTAL_METRICS_NBINS)

    def no_run(self):

        """  do not run the fb, ony returns input workload.. """
        return self.synthetic_workload

    def run(self):

        # config parameters

        # dir for iows and kronos
        fl_iows_dir_input = self.config.FL_IOWS_DIR_INPUT
        fl_iows_dir_output = self.config.FL_IOWS_DIR_OUTPUT
        fl_iows_dir_backup = self.config.FL_IOWS_DIR_BACKUP
        fl_kronos_run_dir = self.config.FL_KRONOS_RUN_DIR

        fl_user_host = self.config.FL_USER_HOST
        fl_user = self.config.FL_USER
        fl_n_iterations = self.config.FL_n_iterations
        fl_log_file = self.config.FL_LOG_FILE

        metrics_names = self.config.metrics_names
        jobs_n_bins = self.config.WORKLOADCORRECTOR_JOBS_NBINS

        # ------------ init DIR ---------------
        if not os.path.exists(fl_iows_dir_backup):
            os.makedirs(fl_iows_dir_backup)

        # initialize the vectors: metric_sums, deltas, etc...
        tuning_factors = {}
        for m in metrics_names:
            tuning_factors[m] = 1.0

        # init log file --------------------
        with open(fl_log_file, "w") as myfile:
            ts_names_str = ''.join(e + ' ' for e in metrics_names)
            myfile.write(ts_names_str + '\n')

        metrics_sums_history = []
        scaling_factors_history = []

        # store input jsons into back-up folder..
        iter0_json_files = [pos_json for pos_json in os.listdir(fl_iows_dir_input) if pos_json.endswith('.json')]
        for i_file in iter0_json_files:
            print "back-up of file: ", i_file
            subprocess.Popen(["cp", fl_iows_dir_input+"/"+i_file, fl_iows_dir_backup]).wait()

        metrics_sum_dict_ref = self.synthetic_workload.total_metrics_dict
        sa_metric_dict = self.synthetic_workload.total_metrics_dict

        print "ts names          : ", metrics_names
        print "reference metrics : ", utils.sort_dict_list(metrics_sum_dict_ref, metrics_names)
        print "sa tot metrics    : ", utils.sort_dict_list(sa_metric_dict, metrics_names)

        # write log file
        write_log_file(fl_log_file, metrics_sum_dict_ref, tuning_factors, metrics_names)

        # /////////////////////////////////////// main loop.. ////////////////////////////////////////////
        for i_count in range(0, fl_n_iterations):

            print "=======================================> ITERATION: ", i_count

            # move the synthetic apps output into kernel dir (and also into bk folder..)
            files = glob.iglob(os.path.join(fl_iows_dir_output, "*.json"))
            for ff, i_file in enumerate(files):
                if os.path.isfile(i_file):
                    # print "scp "+i_file+" "+KRONOS_RUN_DIR+"/input"
                    os.system("scp " + i_file + " " + fl_user_host + ":" + fl_kronos_run_dir + "/input")

                    # store output into back-up folder..
                    os.system("cp " + i_file + " " + fl_iows_dir_backup + "/job_sa-" + str(ff) + "_iter-" + str(
                        i_count + 1) + ".json")

            # ------------------------- run the executor --------------------------------
            subprocess.Popen(["ssh", fl_user_host, fl_kronos_run_dir + "/input/run_jobs"]).wait()
            time.sleep(2.0)
            # ---------------------------------------------------------------------------

            # ..and wait until it completes ----------
            jobs_completed = False
            while not jobs_completed:
                ssh_ls_cmd = subprocess.Popen(["ssh", fl_user_host, "qstat -u "+fl_user],
                                              shell=False,
                                              stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
                jobs_in_queue = ssh_ls_cmd.stdout.readlines()

                if not jobs_in_queue:
                    jobs_completed = True
                    print "jobs completed!"

                time.sleep(2.0)
            # ----------------------------------------

            # ----------------------------------------
            sub_hdl = subprocess.Popen(["ssh", fl_user_host,
                                        "find " + fl_kronos_run_dir + "/output/jobs -name *.map"],
                                       shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            sub_hdl.wait()
            list_map_files = sub_hdl.stdout.readlines()
            for (ff, file_name) in enumerate(list_map_files):
                file_name_ok = file_name.replace("\n", "")
                subprocess.Popen(["scp",
                                  fl_user_host+":"+file_name_ok,
                                  fl_iows_dir_input+"/"+"job-"+str(ff)+".map"]).wait()

                time.sleep(2.0)
                subprocess.Popen(["python",
                                  fl_iows_dir_input+"/map2json.py",
                                  fl_iows_dir_input+"/"+"job-"+str(ff)+".map"]).wait()

            # once all the files have been copied over to local input, rename the host run folder "jobs"
            subprocess.Popen(["ssh", fl_user_host,
                             "mv "+fl_kronos_run_dir+"/output/jobs "+
                             fl_kronos_run_dir+"/output/jobs_iter_"+str(i_count)]).wait()
            # --------------------------------------------------------------------------

            # re-calculate the workload
            time.sleep(5.0)

            # ---------- re read the jobs ----------
            fname_list = []
            for (ff, file_name) in enumerate(list_map_files):
                fname = "job-" + str(ff) + "." + str(i_count + 1) + ".json"
                subprocess.Popen(["cp", fl_iows_dir_input + "/" +fname, fl_iows_dir_backup]).wait()
                fname_list.append(fname)

            # job_datasets = [logreader.ingest_data('allinea', fname, self.config)]
            job_datasets = [profiler_reader.ingest_allinea_profiles(fl_iows_dir_input, jobs_n_bins, fname_list)]

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
                                                         self.config.FL_updatable_metrics)

            print "----------------------- summary: ---------------------------"
            print "ts names            : ", metrics_names
            print "scaling factors     : ", utils.sort_dict_list(tuning_factors, metrics_names)
            print "reference metrics   : ", utils.sort_dict_list(metrics_sum_dict_ref, metrics_names)
            print "metrics sums        : ", utils.sort_dict_list(metrics_sum_dict, metrics_names)
            print "scaling factors new : ", utils.sort_dict_list(sc_factor_dict_new, metrics_names)
            print "------------------------------------------------------------"

            tuning_factors = sc_factor_dict_new

            # update workload through stretching and re-export..
            self.synthetic_workload.set_tuning_factors(tuning_factors)
            self.synthetic_workload.export(self.config.IOWSMODEL_TOTAL_METRICS_NBINS, fl_iows_dir_output)

            # write log file
            write_log_file(fl_log_file, metrics_sum_dict, tuning_factors, metrics_names)
            # -----------------------------------------
        # ///////////////////////////////////// end main loop.. /////////////////////////////////////////////

        # finally rescale the workload according to the calculated tuning factors..
        mean_tuning_factors = {}
        for metric in metrics_names:
            tuning_factors_of_metric = [mm[metric] for mm in scaling_factors_history]
            mean_tuning_factors[metric] = sum(tuning_factors_of_metric)/float(len(tuning_factors_of_metric))

        # apply the refined tuning factors to the workload
            self.synthetic_workload.set_tuning_factors(mean_tuning_factors)

        # return the tuned workload
        return self.synthetic_workload


def get_new_scaling_factors(metrics_ref, metrics_sums, sc_factors, updatable_metrics):
    """ re-calculate scaling factor according to measured deltas.. """
    met_names = updatable_metrics.keys()
    sc_factor_dict_new = {}

    for metric in met_names:
        if metrics_sums[metric] < 1.0e-10 or not updatable_metrics[metric]:
            sc_factor_dict_new[metric] = 1.0
        else:
            sc_factor_dict_new[metric] = metrics_ref[metric] / metrics_sums[metric] * sc_factors[metric]

    return sc_factor_dict_new


def write_log_file(logfile, metrics_sum_dict, scaling_factor_dict, metrics_names):
    """ append metrics sums and scaling factors to log file.. """
    with open(logfile, "a") as myfile:
        metrics_str = ''.join(str(e) + ' ' for e in utils.sort_dict_list(metrics_sum_dict, metrics_names))
        scaling_str = ''.join(str(e) + ' ' for e in utils.sort_dict_list(scaling_factor_dict, metrics_names))
        myfile.write(metrics_str + scaling_str + '\n')

