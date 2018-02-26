# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import subprocess

import numpy as np
import matplotlib.pyplot as plt

try:
    import cPickle as pickle
except:
    import pickle

from kronos.core import time_signal
from kronos.core.config import config
from kronos.core.model import KronosModel
from kronos.core.plot_handler import PlotHandler
from kronos.core.postprocess.plot_model_job import PlotModelJob
from kronos.core.workload_data import WorkloadData
from kronos.io.profile_format import ProfileFormat

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

user_config = {
    "verbose": True,
    "version": 1,
    "uid": 4426,
    "created": "2017-01-27T09:52:10Z",
    "tag": "KRONOS-CONFIG-MAGIC",
    "dir_input": os.path.join(os.getcwd(), "input"),
    "dir_output": os.path.join(os.getcwd(), "output"),
    "kprofile_files": ["dummy_user_wl.kprofile"],
    "ksf_filename": "schedule_epcc.ksf",

    "model": {
        "fill_in": {
            "operations": [{
                "type": "recommender_system",
                "apply_to": ["user_wl"],
                'priority': 2,
                'n_bins': 3
            }]
        },
        "classification": {
            "clustering": {
                "type": "Kmeans",
                "apply_to": ["user_wl"],
                "ok_if_low_rank": True,
                "rseed": 0,
                "max_iter": 100,
                "max_num_clusters": 20,
                "delta_num_clusters": 1,
                "num_timesignal_bins": 3,
                "user_does_not_check": False
            }
        }
    }

}


def user_workload():
    """
    Example function that generates a simplified workload (by explicitly calling kronos-generate-dummy-jobs)
    and applied classification/clustering to it. Some relevant example plots are generated.
    :return:
    """

    dir_output = os.path.dirname(os.path.realpath(__file__))+'/../../../output'
    dir_bin = os.path.dirname(os.path.realpath(__file__))+'/../../../bin'
    kprofile_name_base = "dummy"

    print dir_output
    print dir_bin

    subprocess.Popen([os.path.join(dir_bin, 'kronos-generate-dummy-jobs'),
                      os.path.join(dir_output, kprofile_name_base+'.kprofile')],
                     stdin=None,
                     stdout=None,
                     stderr=None,
                     shell=False).wait()

    # load the KProfile
    kprofile_handle = ProfileFormat.from_filename(os.path.join(dir_output, kprofile_name_base+'.kprofile'))

    # load the additional workload data..
    with open(os.path.join(dir_output, kprofile_name_base+'_class_idx_vec'), "r") as f:
        class_idx_vec = pickle.load(f)

    with open(os.path.join(dir_output, kprofile_name_base+'_model_job_classes'), "r") as f:
        model_job_classes = pickle.load(f)

    # plot jobs classes
    plot_handler = PlotHandler()
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
    plt.savefig(os.path.join(user_config["dir_output"], 'classes.png'))
    plt.close()

    # export wl to a KProfile
    kprofile_handle.write_filename(os.path.join(dir_output, "dummy_user_wl.kprofile"))

    workload_list = [WorkloadData.from_kprofile(kprofile_handle)]
    workload_model = KronosModel(workload_list, config.Config(config_dict=user_config))
    workload_model.generate_model()

    wl = workload_model.workloads[0]

    # plot jobs along with their reference "job-classes"
    for cc, job in enumerate(wl.jobs[:10]):
        class_idx = class_idx_vec[cc]
        class_app = model_job_classes[class_idx]
        PlotModelJob(job).plot(save_fig_name=os.path.join(dir_output, "plot_job-{}".format(cc)),
                               reference_metrics=class_app.timesignals)

    # --------------------- plot clusters.. --------------------------
    print "----> Plotting.."
    model_cluster = workload_model.clusters[0]
    clusters_labels = model_cluster['labels']
    cluster_matrix = model_cluster['cluster_matrix']

    # plot jobs
    n_bins = user_config['model']['classification']['clustering']['num_timesignal_bins']

    plot_handler = PlotHandler()
    fig_size = (24, 16)
    plt.figure(plot_handler.get_fig_handle_ID(), figsize=fig_size, facecolor='w', edgecolor='k')
    ss = 1

    # take only clusters actually found in the labels..
    used_clusters = set(clusters_labels)

    for rr in used_clusters:
        ts_yvalues_all = np.split(cluster_matrix[rr, :], len(time_signal.time_signal_names))
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
    plt.savefig(os.path.join(dir_output, "clusters_rs.png"))
    plt.close()
    # ----------------------------------------------------------------

    print "done!"


if __name__ == '__main__':

    user_workload()
