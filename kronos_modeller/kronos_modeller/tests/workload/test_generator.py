#!/usr/bin/env python
# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest

import numpy as np
from kronos_executor.definitions import time_signal_names
from kronos_modeller.jobs import ModelJob
from kronos_modeller.time_signal.time_signal import TimeSignal
from kronos_modeller.workload import Workload
from kronos_modeller.workload_modelling import generator


class GeneratorTests(unittest.TestCase):

    def test_generator(self):
        """
        The configuration object should have some sane defaults
        """

        # If all of the required arguments are supplied, this should result in a valid job
        ts_complete_set = {tsk: TimeSignal.from_values(tsk, [0., 0.1], [1., 999.])
                           for tsk in time_signal_names}

        valid_args = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': ts_complete_set
        }

        # check that it is a valid job
        job1 = ModelJob(**valid_args)
        job2 = ModelJob(**valid_args)
        job3 = ModelJob(**valid_args)
        job4 = ModelJob(**valid_args)
        job5 = ModelJob(**valid_args)

        input_jobs = [job1, job2, job3, job4, job5]

        # diversify the time start..
        for jj,job in enumerate(input_jobs):
            job.time_start += jj*0.1

        for job in input_jobs:
            self.assertTrue(job.is_valid())

        config_generator = {
                            "type": "match_job_pdf",
                            "random_seed": 0,
                            "scaling_factors": {
                                "kb_collective": 1e-0,
                                "n_collective": 1e-0,
                                "kb_write": 1e-3,
                                "n_pairwise": 10e-0,
                                "n_write": 1e-2,
                                "n_read": 10e-2,
                                "kb_read": 1e-2,
                                "flops": 500e-0,
                                "kb_pairwise": 10e-0
                            },
                            "submit_rate_factor": 8.0,
                            "synthapp_n_cpu": 2,
                            "synthapp_n_nodes": 1,
                            "total_submit_interval": 60
                            }

        # create a workload with 5 model jobs
        test_workload = Workload(
                                    jobs=input_jobs,
                                    tag='test_wl'
                                    )

        # create a matrix from jobs timesignals..
        n_ts_bins = 2
        input_jobs_matrix = test_workload.jobs_to_matrix(n_ts_bins)

        clusters = {
                    'source-workload': test_workload.tag,
                    'jobs_for_clustering': test_workload.jobs,
                    'cluster_matrix': input_jobs_matrix[0, :].reshape(1,input_jobs_matrix.shape[1]),
                    'labels': [1 for job in input_jobs],
                    }

        global_t0 = min(j.time_start for j in clusters['jobs_for_clustering'])
        global_tend = max(j.time_start for j in clusters['jobs_for_clustering'])
        sapps_generator = generator.SyntheticWorkloadGenerator(config_generator, [clusters], global_t0, global_tend)
        modelled_sa_jobs = sapps_generator.generate_synthetic_apps()

        # check that the produced sapps are produced from one of the clusters in the matrix..
        self.assertTrue(any(np.equal(input_jobs_matrix, job.timesignals[ts].yvalues).all(1))
                        for job in modelled_sa_jobs
                        for ts in time_signal_names
                        )

    def test_generator_job_schedule(self):
        """
        test the job time_schedule generation
        :return:
        """

        # If all of the required arguments are supplied, this should result in a valid job
        ts_complete_set = {tsk: TimeSignal.from_values(tsk, [0., 0.1, 0.3], [1., 999., 666.])
                           for tsk in time_signal_names}

        valid_args = {
            'time_start': 0.1,
            'duration': 0.5,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': ts_complete_set
        }

        # create 20 jobs from these time signals
        input_jobs = [ModelJob(**valid_args) for _ in range(20)]

        # diversify the time start..
        for jj, job in enumerate(input_jobs):
            if jj <= 5:
                job.time_start += 10.
            elif 5 < jj < 10:
                job.time_start += 20.
            else:
                job.time_start += 30.

        for job in input_jobs:
            self.assertTrue(job.is_valid())

        # test time_schedule of sa when submit_rate_factor == 1
        config_generator = {
            "type": "match_job_pdf_exact",
            "random_seed": 0,
            "scaling_factors": {
                "kb_collective": 1e-0,
                "n_collective": 1e-0,
                "kb_write": 1e-3,
                "n_pairwise": 10e-0,
                "n_write": 1e-2,
                "n_read": 10e-2,
                "kb_read": 1e-2,
                "flops": 500e-0,
                "kb_pairwise": 10e-0
            },
            "submit_rate_factor": 1.0,
            "synthapp_n_cpu": 2,
            "synthapp_n_nodes": 1,
            "total_submit_interval": max([j.time_start for j in input_jobs])
        }

        # create a workload with 5 model jobs
        test_workload = Workload(
            jobs=input_jobs,
            tag='test_wl'
        )

        # create a matrix from jobs timesignals..
        n_ts_bins = 2
        input_jobs_matrix = test_workload.jobs_to_matrix(n_ts_bins)

        clusters = {
            'source-workload': test_workload.tag,
            'jobs_for_clustering': test_workload.jobs,
            'cluster_matrix': input_jobs_matrix[0, :].reshape(1, input_jobs_matrix.shape[1]),
            'labels': [1 for _ in input_jobs],
        }

        global_t0 = min(j.time_start for j in clusters['jobs_for_clustering'])
        global_tend = max(j.time_start for j in clusters['jobs_for_clustering'])
        sapps_generator = generator.SyntheticWorkloadGenerator(config_generator, [clusters], global_t0, global_tend)

        modelled_sa_jobs = sapps_generator.generate_synthetic_apps()

        xedge_bins = np.linspace(0, max([job.time_start for job in input_jobs]), 10)

        tt_hist, _ = np.histogram(np.asarray([job.time_start for job in input_jobs]), bins=xedge_bins)
        sa_hist, _ = np.histogram(np.asarray([job.time_start for job in modelled_sa_jobs]), bins=xedge_bins)

        # print "[job.time_start for job in input_jobs]", [job.time_start for job in input_jobs]
        # print "[job.time_start for job in modelled_sa_jobs]", [job.time_start for job in modelled_sa_jobs]
        # print "tt_hist", tt_hist
        # print "sa_hist", sa_hist

        # check that the produced sapps are produced from one of the clusters in the matrix..
        self.assertTrue(any(np.equal(input_jobs_matrix, job.timesignals[ts].yvalues).all(1))
                        for job in modelled_sa_jobs
                        for ts in time_signal_names
                        )


if __name__ == "__main__":
    unittest.main()
