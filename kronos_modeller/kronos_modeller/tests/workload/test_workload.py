#!/usr/bin/env python
# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import unittest
from StringIO import StringIO

import numpy as np
from kronos_executor.definitions import signal_types, time_signal_names
from kronos_executor.io_formats.profile_format import ProfileFormat
from kronos_modeller.jobs import ModelJob
from kronos_modeller.time_signal.time_signal import TimeSignal
from kronos_modeller.workload import Workload
from kronos_modeller.workload_filling import StrategyUserDefaults, StrategyMatchKeyword


class WorkloadTests(unittest.TestCase):

    def test_workload_data(self):

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
        for jj, job in enumerate(input_jobs):
            job.time_start += jj * 0.1

        for job in input_jobs:
            self.assertTrue(job.is_valid())

        # create a workload with 5 model jobs
        test_workload = Workload(
                                 jobs=input_jobs,
                                 tag='test_wl'
                                )

        # -- verify that all the jobs in workload are actually the initial jobs provided --
        self.assertTrue(all(job is input_jobs[jj] for jj, job in enumerate(test_workload.jobs)))

        # ------------ verify sums of timesignals -------------------
        for ts_name in signal_types:
            ts_sum = 0
            for j in input_jobs:
                ts_sum += sum(j.timesignals[ts_name].yvalues)

            # verify the sums..
            self.assertEqual(ts_sum, test_workload.total_metrics_sum_dict[ts_name])

        # ------------ verify global time signals -------------------
        valid_args_1 = {
            'time_start': 0.1,
            'duration': 0.222,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in time_signal_names}
        }
        job1 = ModelJob(**valid_args_1)

        valid_args_2 = {
            'time_start': 0.1,
            'duration': 0.333,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in time_signal_names}
        }
        job2 = ModelJob(**valid_args_2)

        test_workload = Workload(jobs=[job1, job2], tag='wl_2jobs')

        for job in [job1, job2]:
            for ts in signal_types:
                self.assertTrue(all(v+job.time_start in test_workload.total_metrics_timesignals[ts].xvalues for v in job.timesignals[ts].xvalues))
                self.assertTrue(all(v in test_workload.total_metrics_timesignals[ts].yvalues for v in job.timesignals[ts].yvalues))
        # -----------------------------------------------------------

    def test_workload_fillin_default(self):
        """
        Test the correct assignment of user-defined time-series
        :return:
        """

        io_metrics = [
            'kb_read',
            'kb_write',
            'n_read',
            'n_write'
        ]

        # create 2 random jobs (with ONLY io metrics)
        valid_args_1 = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in io_metrics}
        }
        job1 = ModelJob(**valid_args_1)

        valid_args_2 = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in io_metrics}
        }
        job2 = ModelJob(**valid_args_2)

        test_workload = Workload(jobs=[job1, job2], tag='wl_2jobs')

        # ---------------------- fill in config -----------------------
        filling_funct_config = [
                                {
                                    "type": "step",
                                    "name": "step-1",
                                    "x_step": 0.5
                                },
                                {
                                    "type": "custom",
                                    "name": "custom-1",
                                    "x_values": [0, 0.1, 0.15, 0.3333, 0.5, 0.8, 0.9, 1.0],
                                    "y_values": [0, 0.1, 0.2, 0.3, 0.5, 0.8, 0.9, 1.0]
                                }
                              ]

        # Values to assign to all the unspecified metrics
        default_config = {
                            "type": "fill_missing_entries",
                            "apply_to": ["wl_2jobs"],
                            "priority": 0,
                            "metrics": {
                                        "kb_collective": [100, 101],
                                        "n_collective": [100, 101],

                                        "kb_pairwise": {"function": "step-1",
                                                        "scaling": 1000.0
                                                        },
                                        "n_pairwise": {"function": "custom-1",
                                                       "scaling": 1000.0
                                                       },
                                        "flops": [100, 101],
                                        }
                        }

        # update the filling config with the user-defined functions
        default_config.update({"user_functions": filling_funct_config})

        # Apply the user defaults to the workloads
        workloads = [test_workload]
        filler = StrategyUserDefaults(workloads)
        filler.apply(default_config)

        # test that the IO metrics are within the random range used [0,1]
        for j in workloads[0].jobs:
            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['n_write'].xvalues]))
            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['n_write'].yvalues]))

            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['kb_write'].xvalues]))
            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['kb_write'].yvalues]))

            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['n_read'].xvalues]))
            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['n_read'].yvalues]))

            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['kb_read'].xvalues]))
            self.assertTrue(all([0.0 < x < 1.0 for x in j.timesignals['kb_read'].yvalues]))

        # test that the user-defined metrics are within the random range chosen [0,1]
        for j in workloads[0].jobs:
            self.assertTrue(all([100 < x < 101 for x in j.timesignals['flops'].yvalues]))
            self.assertTrue(all([100 < x < 101 for x in j.timesignals['n_collective'].yvalues]))
            self.assertTrue(all([100 < x < 101 for x in j.timesignals['kb_collective'].yvalues]))

        # test that the user-defined functions are being applied as expected

        for j in workloads[0].jobs:

            # values vs expected
            val_exp = zip(j.timesignals['n_pairwise'].yvalues,
                          [0, 0.1, 0.2, 0.3, 0.5, 0.8, 0.9, 1.0]
                          )
            self.assertTrue(all([x == y*1000. for x,y in val_exp]))

            # and the step function
            self.assertTrue(all([(x == 0 or x == 1000.)
                                 for x in j.timesignals['kb_pairwise'].yvalues]))

    def test_workload_fillin_match(self):
        """
        Test the metrics assignment through job name (label) matching
        :return:
        """

        # ------------ verify global time signals -------------------
        valid_args_1 = {
            'job_name': "blabla_weird_name",
            'time_start': 0.1,
            'duration': 0.222,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: TimeSignal.from_values(tsk, np.random.rand(10), np.arange(10) * 2)
                            for tsk in time_signal_names}
        }
        job1 = ModelJob(**valid_args_1)

        valid_args_2 = {
            'job_name': "job_match",
            'time_start': 0.1,
            'duration': 0.333,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {}
        }
        job2 = ModelJob(**valid_args_2)

        # ------ target workload (that will receive the time metrics..)
        target_wl = Workload(jobs=[job1, job2], tag='target_workload')

        # ---------- source workload
        valid_args_3 = {
            'job_name': "job_match",
            'time_start': 0.1,
            'duration': 0.333,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in time_signal_names}
        }

        job3 = ModelJob(**valid_args_3)
        source_wl = Workload(jobs=[job3], tag='wl_match_source')

        # filler config
        filler_config = {
            "type": "match_by_keyword",
            "priority": 0,
            "keywords": [
                "job_name"
            ],
            "similarity_threshold": 0.3,
            "source_workloads": [
                "wl_match_source"
            ],
            "apply_to": ["target_workload"]
        }

        # Apply the user defaults to the workloads
        workloads = [target_wl, source_wl]
        filler = StrategyMatchKeyword(workloads)
        filler.apply(filler_config)

        # for ts_k, ts_v in job3.timesignals.iteritems():
        #     print "JOB3:{}:{}".format(ts_k, ts_v.yvalues)
        #
        # for ts_k, ts_v in target_wl.jobs[1].timesignals.iteritems():
        #     print "TRG_J1:{}:{}".format(ts_k, ts_v.yvalues)

        self.assertTrue(all([all(ys == yt
                                 for ys, yt in zip(job3.timesignals[ts_k].yvalues, ts_v.yvalues))
                             for ts_k, ts_v in target_wl.jobs[1].timesignals.iteritems()]))

    def test_reanimate_kprofile(self):
        """
        The purpose of the KProfile is to be able to (re-)animate ModelJobs from the input data.
        """
        valid = {
            "version": 1,
            "tag": "KRONOS-KPROFILE-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
            "workload_tag": "A-tag",
            "profiled_jobs": [{
                "time_start": 537700,
                "time_queued": 99,
                "duration": 147,
                "ncpus": 72,
                "nnodes": 2,
                "time_series": {
                    "kb_read": {
                        "times": [0.01, 0.02, 0.03, 0.04],
                        "values": [15, 16, 17, 18],
                        "priority": 10
                    }
                }
            }]
        }

        pf = ProfileFormat.from_file(StringIO(json.dumps(valid)))

        workload = Workload.from_kprofile(pf)

        self.assertEquals(workload.tag, "A-tag")

        jobs = workload.jobs
        self.assertEquals(len(jobs), 1)
        self.assertIsInstance(jobs[0], ModelJob)

        self.assertEqual(jobs[0].time_start, 537700)
        self.assertEqual(jobs[0].time_queued, 99)
        self.assertEqual(jobs[0].duration, 147)
        self.assertEqual(jobs[0].ncpus, 72)
        self.assertEqual(jobs[0].nnodes, 2)

        self.assertEquals(len(jobs[0].timesignals), len(signal_types))
        self.assertIn('kb_read', jobs[0].timesignals)
        for name, signal in jobs[0].timesignals.iteritems():
            if name == 'kb_read':
                self.assertIsInstance(signal, TimeSignal)
                self.assertTrue(all(x1 == x2 for x1, x2 in zip(signal.xvalues, [0.01, 0.02, 0.03, 0.04])))
                self.assertTrue(all(y1 == y2 for y1, y2 in zip(signal.yvalues, [15, 16, 17, 18])))
            else:
                self.assertIsNone(signal)


if __name__ == "__main__":
    unittest.main()
