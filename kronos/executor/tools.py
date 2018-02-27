# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import copy
import math


def mean_of_list(vals):
    """
    avg value of a list
    :param vals:
    :return:
    """
    return sum(vals)/float(len(vals))


def std_of_list(vals):
    """
    std of a list
    :param vals:
    :return:
    """
    mean_val = mean_of_list(vals)
    return (sum([(v-mean_val)**2 for v in vals])/float(len(vals)))**0.5


def calc_histogram(values, n_bins):
    assert isinstance(n_bins, int)
    assert n_bins > 0

    max_val = max(values)
    min_val = min(values)

    if max_val == min_val:
        return [min_val, max_val], 1
    else:
        delta_bin = (max_val - min_val) / float(n_bins)

        bins = [min_val + i * delta_bin for i in range(n_bins + 1)]
        binned_vals = [0] * n_bins

        for val in values:
            idx_min = int(min(max(math.floor(val / int(delta_bin)), 0), len(binned_vals)-1))

            try:
                binned_vals[idx_min] += 1
            except IndexError:
                raise IndexError

        return bins, binned_vals


class KernelWorkDistribution(object):

    def __init__(self, jobs_data):

        self.jobs_data = jobs_data

    def calculate_per_process_kernel_data(self):
        """
        Create a structure of per-process kernel data (keeping the same frame of the job_data)
        :return:
        """

        kernel_data = copy.deepcopy(self.jobs_data)
        for synth_app in kernel_data:

            nprocs = synth_app["num_procs"]

            for frame in synth_app["frames"]:

                for ker in frame:

                    if ker["name"] == "cpu":
                        ker["work_per_process"] = self.distribute_flops(ker["flops"], nprocs)

                    elif ker["name"] == "file-read":
                        ker["work_per_process"] = []

                    elif ker["name"] == "file-write":
                        ker["work_per_process"] = []

                    elif ker["name"] == "mpi":
                        ker["work_per_process_col"] = self.distribute_mpi(ker["kb_collective"], ker["n_collective"], nprocs)
                        ker["work_per_process_p2p"] = self.distribute_mpi(ker["kb_pairwise"], ker["n_pairwise"], nprocs)

                    else:
                        raise ("kernel name {} not recognized!".format(ker["name"]))

        return kernel_data

    def distribute_flops(self, tot_metric, nprocs):
        """
        Function that mimic the even distribution of FLOPS per kernel
        :param tot_metric:
        :return:
        """
        return [tot_metric/float(nprocs)]*nprocs

    def distribute_mpi(self, tot_metric, n_metric, nprocs):
        """
        Function that mimic the even distribution of MPI ops per kernel
        :param tot_metric:
        :return:
        """
        return [tot_metric/float(n_metric)]*nprocs

    def distribute_io(self, tot_metric, n_metric):
        """
        Function that mimic the even distribution of MPI ops per kernel
        :param tot_metric:
        :return:
        """
        return tot_metric/float(n_metric)


class KernelStats(object):

    small_eps=1.e-10

    def __init__(self, kernel_data):

        self.kernel_data = kernel_data

    def calculate_flops_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        process_flops_stats = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "cpu":
                        process_flops_stats.extend(ker.get("work_per_process", []))

        if not process_flops_stats:
            print "no CPU kernels found in data"
            return None
        else:

            return calc_histogram(process_flops_stats, n_bins)

    def calculate_mpi_col_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        process_flops_stats = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "mpi":
                        process_flops_stats.extend(ker.get("work_per_process_col", []))

        if not process_flops_stats:
            return None
        else:

            return calc_histogram(process_flops_stats, n_bins)

    def calculate_mpi_p2p_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        process_flops_stats = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "mpi":
                        process_flops_stats.extend(ker.get("work_per_process_p2p", []))

        if not process_flops_stats:
            return None
        else:

            return calc_histogram(process_flops_stats, n_bins)

    @staticmethod
    def print_histogram(bins, vals, format_len=25):

        for bb in range(len(bins)-1):
            print "[{:{format_len}}, {:{format_len}}] -> {}".format(bins[bb],
                                                                    bins[bb+1],
                                                                    vals[bb],
                                                                    format_len=format_len)
