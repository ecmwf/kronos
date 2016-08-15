import matplotlib.pyplot as plt
import numpy as np
import argparse
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from plot_handler import PlotHandler
from config import config


def plot_run(ts_names, iows_out_dir, logfile_name=None):

    """ plot a feedback loop run """

    with open(logfile_name, "r") as logfile_name:
        content = logfile_name.readlines()
    content = [x.strip('\n') for x in content]

    # exclude the titles..
    content = content[1:]

    # cast the content
    content = [map(float, row.split()) for row in content]

    # reference metrics (from iteration 0)
    ref_metrics = content[0]

    # remove the ref values from content..
    content = content[1:]
    n_iter = len(content)
    n_metrics = len(ts_names)

    plt.figure(999, figsize=(8, 12), dpi=80, facecolor='w', edgecolor='k')

    for mm, metric in enumerate(ts_names):
        plt.subplot(n_metrics, 1, mm + 1)

        ref_vals = ref_metrics[mm] * np.ones(n_iter)
        iter_vals = np.asarray([row[mm] for row in content])

        xx = np.asarray(range(0, n_iter))
        plt.plot(xx, ref_vals, 'b')
        plt.plot(xx, iter_vals, 'r-*')

        plt.legend(['ref', 'simulated'])
        plt.ylabel(metric)
        plt.xticks(range(0, n_iter + 1))
        plt.xlim(xmin=0, xmax=n_iter + 1.1)

    plt.savefig(iows_out_dir + "/" "_plot_iterations.png")
    # -------------------------------------------------

    # check num plots
    PlotHandler.print_fig_handle_ID()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Plot a metric on a graph")
    parser.add_argument("--fname", help="JSON files with metric data to parse")
    parser.add_argument("--outdir", help="legend associated to plots")
    args = parser.parse_args()

    logfile_name = args.fname
    outdir = args.outdir

    Config = config.Config()
    plot_run(Config.metrics_names, outdir, logfile_name)
