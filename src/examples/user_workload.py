import os

import numpy as np
import matplotlib.pyplot as plt

import generator
import time_signal
from config.config import Config
from plot_handler import PlotHandler
from workload_data import WorkloadData

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from time_signal import TimeSignal, signal_types
from jobs import ModelJob
import data_analysis

# from jobs import model_jobs_from_clusters


def user_workload():

    config_dict = {
                  'dir_input': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output'),
                  'dir_output': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output'),
                  }

    cfg = Config(config_dict=config_dict)
    plot_handler = PlotHandler()

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
    ts_probability = 0.7

    # define number of classes
    # n_procs = 2
    # n_nodes = 1
    # t_length = 20.0
    N_apps = 1000

    # ---------- type of CPU signal (MPI and IO signals are being generated from theser information as well)
    # 0:low,low,low
    # 1:low,high,low
    # 2:high,low,high
    jobs_duration = 3
    time_xvalues = np.linspace(0.0, jobs_duration, 3)
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

    # random distribution of start times
    apps_start_times = np.random.normal(loc=30.0, scale=10.0, size=N_apps)
    print "max start time: {}".format(max(apps_start_times))

    apps_start_times[apps_start_times<0.0] = 0.0

    # //////////////////////////////////// collect job models ///////////////////////////////////////////////
    model_job_list = []
    class_idx_vec = np.zeros(N_apps, dtype=int)
    for app in range(0, N_apps):

        #     app_time_start = np.random.random()*t_length
        app_time_start = apps_start_times[app]

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
            ts_prob = ts_prob_mat[app, tt]

            if ts_prob <= ts_probability:
                ts = TimeSignal(ts_name).from_values(ts_name, time_xvalues, time_yvalues, priority=10)
                timesignals[ts_name] = ts
            else:
                timesignals[ts_name] = None

        job = ModelJob(
            time_start=app_time_start,
            duration=jobs_duration,
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
            for ts_name in time_signal.time_signal_names:
                if ts_name == 'flops':
                    time_yvalues = ops_types[cpu_type] * scale_up_factors['flops'] * overall_level
                elif ts_name != 'flops':
                    time_yvalues = ops_types[mpi_io_type] * scale_up_factors[ts_name] * overall_level

                # create the timesignal
                ts = TimeSignal(ts_name).from_values(ts_name, time_xvalues, time_yvalues)
                timesignals[ts_name] = ts

            job = ModelJob(
                time_start=app_time_start,
                duration=jobs_duration,
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
    plt.figure(plot_handler.get_fig_handle_ID(), figsize=fig_size, facecolor='w', edgecolor='k')
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
    plt.savefig(os.path.join(cfg.dir_output, 'classes.png'))
    plt.close()
    # ----------------------------------------------------------------

    # ---------------- apply recommender system ----------------------
    print "model_job_list", len(model_job_list)

    wl = WorkloadData(jobs=model_job_list,
                      tag="user_wl"
                      )

    rs_config = {
                "type": "recommender_system",
                "priority": 2,
                "n_bins": 3,
                "apply_to": ["operational-ipm"],
                }

    wl.apply_recommender_system(rs_config)
    print "recommendersystem applied!"
    # ----------------------------------------------------------------

    # -------------- plot jobs classes+recomm systems ----------------
    # Number of jobs to plot
    n_jobs_to_plot = 10

    fig_size = (20, 6)
    for cc, app in enumerate(wl.jobs[:n_jobs_to_plot]):

        print "processing app {}".format(cc)
        plt.figure(plot_handler.get_fig_handle_ID(), figsize=fig_size, facecolor='w', edgecolor='k')

        # plot timesignals values from the actual job..
        for tt, ts_name in enumerate(signal_types.keys()):
            plt.subplot(2, len(app.timesignals.keys()), tt + 1)
            if app.timesignals[ts_name]:
                if app.timesignals[ts_name].priority == 10:
                    plt.bar(app.timesignals[ts_name].xvalues, app.timesignals[ts_name].yvalues, 0.5, color='b')
                elif app.timesignals[ts_name].priority == 2:
                    plt.bar(app.timesignals[ts_name].xvalues, app.timesignals[ts_name].yvalues, 0.5, color='g')
                else:
                    raise ValueError(
                        "priority not recognized.. {} for ts {}".format(app.timesignals[ts_name].priority, ts_name))
            else:
                plt.bar(time_xvalues, time_xvalues * 0.0, 0.5, color='k')

            plt.xlabel(ts_name)
            plt.ylabel('')
            plt.gca().xaxis.set_major_locator(plt.NullLocator())

        # plot timesignals values from the CLASS of that job..
        class_idx = class_idx_vec[cc]
        class_app = model_job_classes[class_idx]
        for tt, ts_name in enumerate(signal_types.keys()):
            plt.subplot(2, len(class_app.timesignals.keys()), tt + 1 + len(class_app.timesignals.keys()))
            plt.bar(class_app.timesignals[ts_name].xvalues, class_app.timesignals[ts_name].yvalues, 0.5, color='k')
            plt.xlabel(ts_name)
            plt.ylabel('')

        plt.tight_layout()
        plt.savefig(os.path.join(cfg.dir_output, "classes_with_rs_{}_it2.png".format(cc)))
        plt.close()
    print "figures saved!"
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    clustering_config = {
                        "type": "Kmeans",
                        "ok_if_low_rank": True,
                        "user_does_not_check": True,
                        "rseed": 0,
                        "max_iter": 100,
                        "max_num_clusters": 20,
                        "delta_num_clusters": 1,
                        "num_timesignal_bins": 3,
                        "metrics_only": False
                        }
    print "doing clustering"

    # Apply clustering
    cluster_handler = data_analysis.factory(clustering_config['type'], clustering_config)
    cluster_handler.cluster_jobs(wl.jobs_to_matrix(clustering_config['num_timesignal_bins']))
    clusters_matrix = cluster_handler.clusters
    clusters_labels = cluster_handler.labels
    used_clusters = set(clusters_labels)

    print "clustering done: number of ACTUAL clusters: {}".format(len(used_clusters))
    # ----------------------------------------------------------------

    # --------------------- plot clusters.. --------------------------
    # plot jobs
    n_bins = clustering_config['num_timesignal_bins']
    fig_size = (24, 16)
    plt.figure(plot_handler.get_fig_handle_ID(), figsize=fig_size, facecolor='w', edgecolor='k')
    ss = 1

    # take only clusters actually found in the labels..
    used_clusters = set(clusters_labels)

    for rr in used_clusters:

        ts_yvalues_all = np.split(clusters_matrix[rr, :], len(time_signal.time_signal_names))
        for tt, ts_name in enumerate(time_signal.time_signal_names):
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
    plt.savefig(os.path.join(cfg.dir_output, "clusters_rs.png"))
    plt.close()
    # ----------------------------------------------------------------

    # ----- generate the synthetic workload from the model jobs.. ----
    config_generator = {
                        "type": "match_job_pdf_exact",
                        "random_seed": 0,
                        "tuning_factors": {
                                          "kb_collective": 1e-0,
                                          "n_collective": 1e-0,
                                          "kb_write": 1e-3,
                                          "n_pairwise": 1e-0,
                                          "n_write": 1e-2,
                                          "n_read": 1e-2,
                                          "kb_read": 1e-2,
                                          "flops": 100e-0,
                                          "kb_pairwise": 1e-0
                                          },
                       "submit_rate_factor": 1.0,
                       "synthapp_n_cpu": 2,
                       "synthapp_n_nodes": 1,
                       "synthapp_n_frames": 10,
                       "total_submit_interval": max(apps_start_times)
                       }

    clusters_dict = [{
                    'source-workload': 'wl_user',
                    'jobs_for_clustering': wl.jobs,
                    'cluster_matrix': clusters_matrix,
                    'labels': clusters_labels,
                    }]

    # sapps_generator = generator.SyntheticWorkloadGenerator(config_generator, clusters_dict)

    global_t0 = min(j.time_start for cl in clusters_dict for j in cl['jobs_for_clustering'])
    global_tend = max(j.time_start for cl in clusters_dict for j in cl['jobs_for_clustering'])
    sapps_generator = generator.SyntheticWorkloadGenerator(config_generator, clusters_dict, global_t0, global_tend)

    modelled_sa_jobs = sapps_generator.generate_synthetic_apps()

    # ------------ plot real/synthetic job distribution ---------------
    xedge_bins = np.linspace(0.0, max(apps_start_times), 21)
    xx = 0.5*(xedge_bins[1:]+xedge_bins[:-1])
    delta = xx[1]-xx[0]

    # real start times
    tt_hist = np.histogram(apps_start_times, bins=xedge_bins)

    print "---------------"
    print "xedge_bins", xedge_bins
    print "tt_hist", tt_hist[0]

    # synthetic start times
    sa_hist = np.histogram(np.asarray([j.time_start for j in modelled_sa_jobs]), bins=xedge_bins)
    print "sa_hist", sa_hist[0]

    plt.figure(plot_handler.get_fig_handle_ID())
    plt.bar(xx-0.25*delta, tt_hist[0], delta/2., color='b')
    plt.bar(xx+0.25*delta, sa_hist[0], delta/2., color='r')
    plt.xlabel('start time')
    plt.ylabel('# jobs')
    plt.savefig( os.path.join(cfg.dir_output, 'jobs_distribution.png'))
    plt.close()
    # -----------------------------------------------------------------

if __name__ == '__main__':

    user_workload()
