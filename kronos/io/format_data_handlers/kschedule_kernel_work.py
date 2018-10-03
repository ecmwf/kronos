# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos.core.app_kernels import available_kernels


def distribute_int_kernel_ops(n_procs, nelems, mpi_rank, work_accumulator):
    """
    Distribute an integer number of ops to processes as evenly as possible
    :param n_procs:
    :param nelems:
    :param mpi_rank:
    :param work_accumulator:
    :return:
    """

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

    """
    Class that handles the work distribution among processes (as done by the synth apps)
    """

    metric_aggregation_options = {

        "flops": ["process"],

        "n_read": ["process"],
        "kb_read": ["process", "call"],
        "n_write": ["process"],
        "kb_write": ["process", "call"],

        "n_pairwise": ["process"],
        "kb_pairwise": ["process", "call"],
        "n_collective": ["process"],
        "kb_collective": ["process", "call"],

        "kb_mem": ["process"],
    }

    def __init__(self, jobs_data):

        self.jobs_data = jobs_data

    def calculate_sub_kernel_distribution(self, metric_name, aggregation_type):
        """
        Create a structure of per-process kernel data (keeping the same frame of the job_data)
        :return:
        """

        # check that the aggregation type is possible for the requested metric
        if aggregation_type not in self.metric_aggregation_options[metric_name]:
            raise RuntimeError("metric: {} cannot be aggregated per {}".format(metric_name, aggregation_type))

        # name of the kernels to read..
        kernel_params = {ker.name: [s[0] for s in ker.signals] for ker in available_kernels}

        # kernel type associated to each metric (e.g. kernel_type_for_metric["kb_write"] = "file-write")
        kernel_type_for_metric = {p:ker_name for ker_name, params in kernel_params.iteritems() for p in params}

        # kernel type for the metric requested
        kernel_type_of_requested_metric = kernel_type_for_metric[metric_name]

        _full_series = []
        for synth_app in self.jobs_data:

            nprocs = synth_app["num_procs"]

            for frame in synth_app["frames"]:

                for ker in frame:

                    if ker["name"] == kernel_type_of_requested_metric:

                        if ker["name"] == "cpu":
                            if aggregation_type == "process":
                                _full_series.extend([ker["flops"]/float(nprocs)] * nprocs)

                        elif ker["name"] == "file-read":
                            _work_per_process, _work_per_call, _calls_per_proc = self.distribute_kernel_int(ker["kb_read"], ker["n_read"], nprocs)

                            if metric_name == "kb_read" and aggregation_type == "process":
                                _full_series.extend(_work_per_process)
                            elif metric_name == "kb_read" and aggregation_type == "call":
                                _full_series.extend(_work_per_call)
                            elif metric_name == "n_read" and aggregation_type == "process":
                                _full_series.extend(_calls_per_proc)

                        elif ker["name"] == "file-write":
                            _work_per_process, _work_per_call, _calls_per_proc = self.distribute_kernel_int(ker["kb_write"], ker["n_write"], nprocs)

                            if metric_name == "kb_write" and aggregation_type == "process":
                                _full_series.extend(_work_per_process)
                            elif metric_name == "kb_write" and aggregation_type == "call":
                                _full_series.extend(_work_per_call)
                            elif metric_name == "n_write" and aggregation_type == "process":
                                _full_series.extend(_calls_per_proc)

                        elif ker["name"] == "mpi":

                            if metric_name == "kb_collective" and aggregation_type == "process":
                                _full_series.extend([ker["kb_collective"]] * nprocs)
                            elif metric_name == "kb_collective" and aggregation_type == "call":
                                _full_series.extend([ker["kb_collective"]/float(ker["n_collective"])] * ker["n_collective"])
                            elif metric_name == "n_collective" and aggregation_type == "process":
                                _full_series.extend([ker["n_collective"]]*nprocs)

                            if metric_name == "kb_pairwise" and aggregation_type == "process":
                                _full_series.extend([ker["kb_pairwise"]] * nprocs)
                            elif metric_name == "kb_pairwise" and aggregation_type == "call":
                                _full_series.extend([ker["kb_pairwise"]/float(ker["n_pairwise"])] * ker["n_pairwise"])
                            elif metric_name == "n_pairwise" and aggregation_type == "process":
                                _full_series.extend([ker["n_pairwise"]]*nprocs)

                        else:
                            raise ("kernel name {} not recognized!".format(ker["name"]))

        return _full_series

    def distribute_kernel_int(self, tot_metric_per_kernel, n_ops_per_kernel, nprocs):
        """
        Sizes of *IO calls*
        :param tot_metric_per_kernel:
        :param n_ops_per_kernel:
        :param nprocs:
        :return:
        """

        assert tot_metric_per_kernel > 0
        assert n_ops_per_kernel > 0
        assert nprocs > 0

        # #calls per process
        _calls_per_proc = []
        work_acc = 0
        for i_proc in range(nprocs):
            _n_ops, work_acc = distribute_int_kernel_ops(nprocs, n_ops_per_kernel, i_proc, work_acc)
            _calls_per_proc.append(_n_ops)

        # the ops-sizes are equal to tot_metric_per_kernel/nprocs/n_local_count
        tot_metric_per_proc = tot_metric_per_kernel/float(nprocs)

        # work per processor
        _work_per_process = [tot_metric_per_proc]*nprocs

        # work per call
        _work_per_call = []
        for c in _calls_per_proc:
            if c:
                _work_per_call.extend([tot_metric_per_proc/float(c)]*c)

        return _work_per_process, _work_per_call, _calls_per_proc
