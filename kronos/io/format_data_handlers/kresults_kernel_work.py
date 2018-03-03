# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import copy

from kronos.shared_tools.shared_utils import calc_histogram


def distribute_kernel_work(n_procs, nelems, mpi_rank, work_accumulator):

    nelems_local = nelems / n_procs
    accumulator_new = (work_accumulator + nelems) % n_procs

    if accumulator_new > work_accumulator:

        if work_accumulator <= mpi_rank < accumulator_new:
            nelems_local += 1

    elif accumulator_new < work_accumulator:

        if mpi_rank >= work_accumulator or mpi_rank < accumulator_new:
            nelems_local += 1

    work_accumulator = accumulator_new

    return nelems_local, work_accumulator


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
                        ker["flops_per_process"] = self.distribute_flops(ker["flops"], nprocs)

                    elif ker["name"] == "file-read":
                        ker["read_sizes_per_process"] = self.distribute_io(ker["kb_read"], ker["n_read"], nprocs)

                    elif ker["name"] == "file-write":
                        ker["write_sizes_per_process"] = self.distribute_io(ker["kb_write"], ker["n_write"], nprocs)

                    elif ker["name"] == "mpi":
                        ker["coll_sizes_per_process_col"] = self.distribute_mpi(ker["kb_collective"], ker["n_collective"], nprocs)
                        ker["p2p_sizes_per_process_p2p"] = self.distribute_mpi(ker["kb_pairwise"], ker["n_pairwise"], nprocs)

                    else:
                        raise ("kernel name {} not recognized!".format(ker["name"]))

        return kernel_data

    def distribute_flops(self, tot_metric_per_kernel, nprocs):
        """
        Sizes per process work
        :param tot_metric_per_kernel:
        :return:
        """
        return [tot_metric_per_kernel / float(nprocs)] * nprocs

    def distribute_mpi(self, tot_metric_per_kernel, n_ops_per_kernel, nprocs):
        """
        Sizes of *MPI calls*
        :param tot_metric_per_kernel:
        :param n_ops_per_kernel:
        :param nprocs
        :return:
        """
        return [tot_metric_per_kernel / float(n_ops_per_kernel)] * nprocs

    def distribute_io(self, tot_metric_per_kernel, n_ops_per_kernel, nprocs):
        """
        Sizes of *IO calls*
        :param tot_metric_per_kernel:
        :param n_ops_per_kernel:
        :param nprocs:
        :return:
        """

        # calculate #nreads per process
        n_ops_per_proc = []
        work_acc = 0
        for i_proc in range(nprocs):
            _n_ops, work_acc = distribute_kernel_work(nprocs, n_ops_per_kernel, i_proc, work_acc)
            n_ops_per_proc.append(_n_ops)

        # the ops-sizes are equal to tot_metric_per_kernel/nprocs/n_local_count
        tot_metric_per_proc = tot_metric_per_kernel/float(nprocs)
        return [tot_metric_per_proc/float(c) if c else 0 for c in n_ops_per_proc]


class KernelStats(object):

    small_eps=1.e-10

    def __init__(self, kernel_data):

        self.kernel_data = kernel_data

    def calculate_flops_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        values = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "cpu":
                        values.extend(ker.get("flops_per_process", []))

        if not values:
            return None
        else:

            return calc_histogram(values, n_bins)

    def calculate_mpi_col_calls_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        values = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "mpi":
                        values.extend(ker.get("coll_sizes_per_process_col", []))

        if not values:
            return None
        else:

            return calc_histogram(values, n_bins)

    def calculate_mpi_p2p_calls_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        values = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "mpi":
                        values.extend(ker.get("p2p_sizes_per_process_p2p", []))

        if not values:
            return None
        else:

            return calc_histogram(values, n_bins)

    def calculate_io_read_calls_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        values = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "file-read":
                        values.extend(ker.get("read_sizes_per_process", []))

        if not values:
            return None
        else:

            return calc_histogram(values, n_bins)

    def calculate_io_write_calls_histograms(self, n_bins=10):
        """
        calculate statistics from the per-process data
        :return:
        """

        values = []
        for synth_app in self.kernel_data:
            for frame in synth_app["frames"]:
                for ker in frame:
                    if ker["name"] == "file-write":
                        values.extend(ker.get("write_sizes_per_process", []))

        if not values:
            return None
        else:

            return calc_histogram(values, n_bins)

    @staticmethod
    def print_histogram(bins, vals, format_len=25):

        for bb in range(len(bins)-1):
            print "[{:{format_len}}, {:{format_len}}] -> {}".format(bins[bb],
                                                                    bins[bb+1],
                                                                    vals[bb],
                                                                    format_len=format_len)
