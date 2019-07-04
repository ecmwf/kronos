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
