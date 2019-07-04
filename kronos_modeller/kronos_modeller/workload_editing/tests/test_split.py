# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import copy
import unittest

import numpy as np
from kronos_executor.definitions import time_signal_names
from kronos_modeller.jobs import ModelJob
from kronos_modeller.time_signal.time_signal import TimeSignal
from kronos_modeller.workload import Workload
from kronos_modeller.workload_editing import WorkloadSplit


class SplitterTests(unittest.TestCase):
    """
    Some tests of the workload splitting functionality
    """

    def test_splitter(self):

        # -------------- prepare a couple of dummy jobs ---------------

        # If all of the required arguments are supplied, this should result in a valid job
        ts_complete_set = {tsk: TimeSignal.from_values(tsk, [0., 0.1], [1., 999.])
                           for tsk in time_signal_names}

        ts_complete_set_2 = {tsk: TimeSignal.from_values(tsk, [0., 0.1], [1., 444.])
                             for tsk in time_signal_names}

        valid_args = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': ts_complete_set,
            'job_name': "job_name_1"
        }

        valid_args_2 = {
            'time_start': 0.2,
            'duration': 0.4,
            'ncpus': 2,
            'nnodes': 2,
            'timesignals': ts_complete_set_2,
            'job_name': "job_name_2"
        }

        # a model job that WILL NOT be picked by the algorithm..
        job1 = ModelJob(**valid_args)
        job1.label = "label_nottobepicked"

        # a model job that WILL be picked by the algorithm..
        job2 = ModelJob(**valid_args_2)
        job2.label = "label_includeme"

        # dummy workload with 20 jobs
        np.random.seed(0)
        jobs_all = []
        for i in range(20):

            # spawn a new job from either job1 or job2
            if np.random.rand() < 0.5:
                new_job = copy.deepcopy(job1)
            else:
                new_job = copy.deepcopy(job2)

            # assign it a new label
            jobs_all.append(new_job)

        # create a workload out of all the jobs..
        workload = Workload(jobs=jobs_all, tag="testing_workload")

        # configure the splitter from user config
        config_splitting = {
            "type": "split",
            "keywords_in": ["includeme"],
            "keywords_out": ["excludeme"],
            "split_by": "label",
            "apply_to": ["testing_workload"],
            "create_workload": "spawn_workload"
        }

        workloads = [workload]
        splitter = WorkloadSplit(workloads)
        splitter.apply(config_splitting)

        wl_out = None
        for wl in workloads:
            if wl.tag == config_splitting["create_workload"]:
                wl_out = wl
                break

        # make sure that we have created a workload as expected
        self.assertTrue(wl_out is not None)
        self.assertEquals(wl_out.tag, config_splitting["create_workload"])

        # make sure that all the jobs have a label consistent with the filter
        for j in wl_out.jobs:
            self.assertTrue("includeme" in j.label and "excludeme" not in j.label)


if __name__ == "__main__":
    unittest.main()

