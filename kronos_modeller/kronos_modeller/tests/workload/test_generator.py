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
from kronos_modeller.workload_modelling import workload_modelling_types


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

        ts_complete_set_2 = {tsk: TimeSignal.from_values(tsk, [0., 0.1], [1., 444.])
                           for tsk in time_signal_names}

        valid_args_2 = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': ts_complete_set_2
        }

        # check that it is a valid job
        job1 = ModelJob(**valid_args)
        job1.label = "job1"

        job2 = ModelJob(**valid_args_2)
        job2.label = "job2"

        job3 = ModelJob(**valid_args)
        job3.label = "job3"

        job4 = ModelJob(**valid_args_2)
        job4.label = "job4"

        job5 = ModelJob(**valid_args)
        job5.label = "job5"

        input_jobs = [job1, job2, job3, job4, job5]

        # diversify the time start..
        for jj,job in enumerate(input_jobs):
            job.time_start += jj*0.1

        for job in input_jobs:
            self.assertTrue(job.is_valid())

        config_generator = {
                "type": "cluster_and_spawn",
                "job_clustering": {
                    "type": "Kmeans",
                    "rseed": 0,
                    "apply_to": ["test_wl_0"],
                    "ok_if_low_rank": True,
                    "max_iter": 100,
                    "max_num_clusters": 3,
                    "delta_num_clusters": 1,
                    "num_timesignal_bins": 1,
                    "user_does_not_check": True
                },
                "job_submission_strategy": {
                    "type": "match_job_pdf_exact",
                    "n_bins_for_pdf": 20,
                    "submit_rate_factor": 8,
                    "total_submit_interval": 60,
                    "random_seed": 0
                }
        }

        # select the appropriate workload_filling strategy
        workloads = [
            Workload(jobs=input_jobs, tag='test_wl_0'),
            Workload(jobs=input_jobs, tag='test_wl_1'),
            Workload(jobs=input_jobs, tag='test_wl_2')
        ]

        workload_modeller = workload_modelling_types[config_generator["type"]](workloads)

        workload_modeller.apply(config_generator)

        # get the newly created set of (modelled) workloads
        workload_set = workload_modeller.get_workload_set()

        # # check that the produced sapps are produced from one of the clusters in the matrix..
        # self.assertTrue(any(np.equal(input_jobs_matrix, job.timesignals[ts].yvalues).all(1))
        #                 for job in modelled_sa_jobs
        #                 for ts in time_signal_names
        #                 )

if __name__ == "__main__":
    unittest.main()
