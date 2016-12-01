import os
import pprint

import numpy as np
import matplotlib.pyplot as plt

import runner
from kronos_tools.print_colour import print_colour
from time_signal import TimeSignal, signal_types
from jobs import ModelJob
from synthetic_app import SyntheticApp, SyntheticWorkload

import data_analysis
from data_analysis import recommender
from plugins.plugin_base import PluginBase
from jobs import model_jobs_from_clusters


class PluginUSER(PluginBase):
    """
    just a simple user-defined workload generated from predefined classes of apps.
    This plugin is intended to be a HARDCODED example to show how the recommender system
    and the clustering algorithm work on simplified jobs.
    """
    def __init__(self, config):
        super(PluginUSER, self).__init__(config)

    def ingest_data(self):
        raise NotImplementedError("ARCTUR plugin not yet implemented")

    def generate_model(self):

        # factors to "scale up" each generated job..
        scale_up_factors = {
            "kb_collective": 1.0e3,
            "n_collective": 1.0e3,
            "kb_write": 1.0e3,
            "n_pairwise": 1.0e3,
            "n_write": 1.0e3,
            "n_read": 1.0e3,
            "kb_read": 1.0e3,
            "flops": 1.0e10,
            "kb_pairwise": 1.0e3
        }

        # probability of missing signal = 0.8 # 0 ->no ts available, 1 -> all ts available
        # ts_probability = 1.0
        ts_probability = 1.0

        # define number of classes
        # n_procs = 2
        # n_nodes = 1
        # t_length = 20.0
        N_apps = 100

        # ---------- type of CPU signal (MPI and IO signals are being generated from theser information as well)
        # 0:low,low,low
        # 1:low,high,low
        # 2:high,low,high
        time_xvalues = np.asarray([0, 1, 2])
        ops_types = {
            0: np.asarray([0.5, 0.5, 0.5]),
            1: np.asarray([0.1, 1, 0.1]),
            2: np.asarray([1., 0.1, 1]),
        }

        # --------------- overall level of signals
        # 0:low
        # 1:medium
        # 2:high
        ops_level = np.asarray([1, 4, 8])

        # combinations of signals type and level (for class )
        class_idx_set = [(ot, ol) for ot in ops_types for ol in ops_level]

        # create all the random indices in one go
        np.random.seed(0)
        ts_prob_mat = np.random.random(size=(N_apps, len(signal_types.keys())))
        cpu_type_vec = np.random.randint(len(ops_types), size=N_apps)
        overall_level_idx_vec = np.random.randint(len(ops_level), size=N_apps)
        # //////////////////////////////////// collect job models ///////////////////////////////////////////////
        model_job_list = []
        class_idx_vec = np.zeros(N_apps, dtype=int)
        for app in range(0, N_apps):

            #     app_time_start = np.random.random()*t_length
            app_time_start = 0.0

            #     cpu_type = np.random.randint(len(ops_types))
            cpu_type = cpu_type_vec[app]
            overall_level = ops_level[overall_level_idx_vec[app]]
            n_nodes = overall_level_idx_vec[app] + 1
            n_procs = n_nodes * 24

            class_idx_vec[app] = class_idx_set.index((cpu_type, overall_level))
            if class_idx_vec[app] not in range(0, 9):
                raise ValueError("class index out of range!")

            if cpu_type == 0:
                mpi_io_type = 0
            elif cpu_type == 1:
                mpi_io_type = 2
            elif cpu_type == 2:
                mpi_io_type = 1

            timesignals = {}
            for tt, ts_name in enumerate(signal_types.keys()):
                if ts_name == 'flops':
                    time_yvalues = ops_types[cpu_type] * scale_up_factors['flops'] * overall_level
                elif ts_name != 'flops':
                    time_yvalues = ops_types[mpi_io_type] * scale_up_factors[ts_name] * overall_level

                # create the timesignal
                ts_prob = np.random.random()
                ts_prob = ts_prob_mat[app, tt]

                if ts_prob <= ts_probability:
                    ts = TimeSignal(ts_name).from_values(ts_name, time_xvalues, time_yvalues, priority=10)
                    timesignals[ts_name] = ts
                else:
                    timesignals[ts_name] = None

            job = ModelJob(
                time_start=app_time_start,
                duration=3.0,
                ncpus=n_procs,
                nnodes=n_nodes,
                timesignals=timesignals,
                label="app_id-{}".format(app)
            )

            model_job_list.append(job)

        print "done jobs!"
        # ----------------------------------------------------------------

        # --------------------- collect job classes ----------------------
        model_job_classes = []
        count = 0
        for cpu_type in range(0, len(ops_types)):
            for overall_level in ops_level:

                n_nodes = overall_level + 1
                n_procs = n_nodes * 24
                app_time_start = 0.0

                if cpu_type == 0:
                    mpi_io_type = 0
                elif cpu_type == 1:
                    mpi_io_type = 2
                elif cpu_type == 2:
                    mpi_io_type = 1

                timesignals = {}
                for ts_name in signal_types.keys():
                    if ts_name == 'flops':
                        time_yvalues = ops_types[cpu_type] * scale_up_factors['flops'] * overall_level
                    elif ts_name != 'flops':
                        time_yvalues = ops_types[mpi_io_type] * scale_up_factors[ts_name] * overall_level

                    # create the timesignal
                    ts = TimeSignal(ts_name).from_values(ts_name, time_xvalues, time_yvalues)
                    timesignals[ts_name] = ts

                job = ModelJob(
                    time_start=app_time_start,
                    duration=3.0,
                    ncpus=n_procs,
                    nnodes=n_nodes,
                    timesignals=timesignals,
                    label="app_id-{}".format(count)
                )

                model_job_classes.append(job)

                count += 1

        print "done classes!"
        # ----------------------------------------------------------------

        # ---------------------- plot jobs classes -----------------------
        fig_size = (24, 16)
        plt.figure(211, figsize=fig_size, facecolor='w', edgecolor='k')
        ss = 1
        for cc, app in enumerate(model_job_classes):
            for tt, ts_name in enumerate(app.timesignals.keys()):
                plt.subplot(len(model_job_classes), len(app.timesignals.keys()), ss)
                plt.bar(app.timesignals[ts_name].xvalues, app.timesignals[ts_name].yvalues, 0.5, color='b')
                plt.xlabel('')
                plt.ylabel('')
                plt.gca().xaxis.set_major_locator(plt.NullLocator())
                ss += 1
                if cc == len(model_job_classes) - 1:
                    plt.xlabel(ts_name)

        plt.tight_layout()
        plt.savefig(os.path.join(self.config.dir_output, 'classes.png'))
        plt.close()
        # ----------------------------------------------------------------

        # ---------------- apply recommender system ----------------------
        print "model_job_list", len(model_job_list)
        recomm_sys = recommender.Recommender()
        recomm_sys.train_model(model_job_list, n_ts_bins=3)
        model_job_list_mod = recomm_sys.apply_model_to(model_job_list)
        print "recommendersystem applied!"
        # ----------------------------------------------------------------

        # # -------------- plot jobs classes+recomm systems ----------------
        # # Number of jobs to plot
        # n_jobs_to_plot = 10
        #
        # fig_size = (20, 6)
        # for cc, app in enumerate(model_job_list_mod[:n_jobs_to_plot]):
        #
        #     print "processing app {}".format(cc)
        #     plt.figure(cc, figsize=fig_size, facecolor='w', edgecolor='k')
        #
        #     # plot timesignals values from the actual job..
        #     for tt, ts_name in enumerate(signal_types.keys()):
        #         plt.subplot(2, len(app.timesignals.keys()), tt + 1)
        #         if app.timesignals[ts_name]:
        #             if app.timesignals[ts_name].priority == 10:
        #                 plt.bar(app.timesignals[ts_name].xvalues, app.timesignals[ts_name].yvalues, 0.5, color='b')
        #             elif app.timesignals[ts_name].priority == 2:
        #                 plt.bar(app.timesignals[ts_name].xvalues, app.timesignals[ts_name].yvalues, 0.5, color='g')
        #             else:
        #                 raise ValueError(
        #                     "priority not recognized.. {} for ts {}".format(app.timesignals[ts_name].priority, ts_name))
        #         else:
        #             plt.bar(time_xvalues, time_xvalues * 0.0, 0.5, color='k')
        #
        #         plt.xlabel(ts_name)
        #         plt.ylabel('')
        #         plt.gca().xaxis.set_major_locator(plt.NullLocator())
        #
        #     # plot timesignals values from the CLASS of that job..
        #     class_idx = class_idx_vec[cc]
        #     class_app = model_job_classes[class_idx]
        #     for tt, ts_name in enumerate(signal_types.keys()):
        #         plt.subplot(2, len(class_app.timesignals.keys()), tt + 1 + len(class_app.timesignals.keys()))
        #         plt.bar(class_app.timesignals[ts_name].xvalues, class_app.timesignals[ts_name].yvalues, 0.5, color='k')
        #         plt.xlabel(ts_name)
        #         plt.ylabel('')
        #
        #     plt.tight_layout()
        #     plt.savefig(os.path.join(self.config.dir_output, "classes_with_rs_{}_it2.png".format(cc)))
        #     plt.close()
        # print "figures saved!"
        # # ----------------------------------------------------------------

        print "doing clustering"
        clustering_config = self.config.plugin["clustering"]
        cluster_handler = data_analysis.factory("Kmeans", clustering_config)
        cluster_handler.cluster_jobs(model_job_list_mod)
        clusters_matrix = cluster_handler.clusters
        clusters_labels = cluster_handler.labels
        used_clusters = set(clusters_labels)
        print "clustering done: number of ACTUAL clusters: {}".format(len(used_clusters))
        # ----------------------------------------------------------------

        # --------------------- plot clusters.. --------------------------
        # plot jobs
        n_bins = clustering_config['n_ts_bins']
        fig_size = (24, 16)
        plt.figure(411, figsize=fig_size, facecolor='w', edgecolor='k')
        ss = 1

        # take only clusters actually found in the labels..
        used_clusters = set(clusters_labels)

        # timesignal_names:
        ts_names_list = signal_types.keys()

        for rr in used_clusters:

            ts_yvalues_all = np.split(clusters_matrix[rr, :], len(signal_types))
            for tt, ts_name in enumerate(ts_names_list):
                ts_values = ts_yvalues_all[tt]
                plt.subplot(len(used_clusters), len(ts_yvalues_all), ss)
                plt.bar(np.arange(0, n_bins), ts_values, 0.5, color='r')
                plt.xlabel('')
                plt.ylabel('')
                plt.gca().xaxis.set_major_locator(plt.NullLocator())
                ss += 1
                if rr == len(used_clusters) - 1:
                    plt.xlabel(ts_name)

        plt.tight_layout()
        plt.savefig(os.path.join(self.config.dir_output, "clusters_rs.png"))
        plt.close()
        # ----------------------------------------------------------------

        # ----------- create synthetic apps from clusters.. --------------
        n_export_sa = 4
        job_from_clusters_list = model_jobs_from_clusters(clusters_matrix,
                                                          clusters_labels,
                                                          np.zeros(n_export_sa),
                                                          nprocs=2,
                                                          nnodes=1
                                                          )

        operational_sa_list = []
        for cc, job in enumerate(job_from_clusters_list):
            app = SyntheticApp(
                job_name="RS-appID-{}".format(cc),
                time_signals=job.timesignals,
                ncpus=2,
                nnodes=1,
                time_start=job.time_start
            )
            operational_sa_list.append(app)

        sa_workload = SyntheticWorkload(self.config, apps=operational_sa_list)
        sa_workload.set_tuning_factors(self.config.plugin['tuning_factors'])
        sa_workload.export_ksf(3, os.path.join(self.config.dir_output, 'schedule.ksf') )
        # ----------------------------------------------------------------

        print " -------- model sums ---------"
        pp = pprint.PrettyPrinter(depth=4)
        model_sums = self.calculate_sums(model_job_list_mod)
        pp.pprint(model_sums)

        print " -------- sa sums ---------"
        pp = pprint.PrettyPrinter(depth=4)
        sa_sums = self.calculate_sums(operational_sa_list)
        pp.pprint(sa_sums)

    def run(self):
        """
        run model of workload
        :return:
        """
        print_colour("green", "running ecmwf plugin..")
        ecmwf_runner = runner.factory(self.config.runner['type'], self.config)
        ecmwf_runner.run()

    def postprocess(self, postprocess_flag):
        """
        run post-process
        :return:
        """
        print_colour("green", "running ecmwf postprocessing..")
        super(PluginUSER, self).postprocess(postprocess_flag)

    def calculate_sums(self, jobs_list):
        sums_dict = {}
        for jj,job in enumerate(jobs_list):
            for k, v in job.timesignals.items():
                if v:
                    try:
                        sums_dict[k] += v.sum
                    except KeyError:
                        sums_dict[k] = v.sum
                else:
                    print job.timesignals.items()
                    raise ValueError("ts {} of job {} is None!".format(k, jj))

        return sums_dict
