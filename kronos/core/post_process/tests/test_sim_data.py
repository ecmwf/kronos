#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest
import numpy as np

from kronos.core.post_process.krf_data import KRFJob
from kronos.core.post_process.krf_decorator import KRFDecorator
from kronos.core.post_process.sim_data import SimulationData
from kronos.core.post_process.tests.test_utils import create_krf


class SimDataTest(unittest.TestCase):

    def test_sim_metadata(self):

        time_series_2proc = [
            {
                "flops": np.asarray([0, 111, 0, 111]),
                "bytes_read": np.asarray([333.3, 0, 0, 1]),
                "n_read": np.asarray([3, 0, 0, 1]),
                "bytes_write": np.asarray([0, 444.4, 0, 1]),
                "n_write": np.asarray([0, 2, 0, 1]),
                "durations": np.asarray([1.0, 1.0, 1.0, 1.0])
            },
            {
                "flops": np.asarray([222, 0, 0, 222]),
                "bytes_read": np.asarray([0, 0, 0, 1]),
                "n_read": np.asarray([0, 0, 0, 1]),
                "bytes_write": np.asarray([0, 0, 0, 1]),
                "n_write": np.asarray([0, 0, 0, 1]),
                "durations": np.asarray([1.0, 1.0, 1.0, 6.0])
            }
        ]

        # crete krf data (job-0)
        krf_json_data_0 = create_krf(time_series_2proc, creation_date="2017-07-31T01:28:42+00:00")
        decor_data = KRFDecorator(workload_name="workload_1/something_else/parallel", job_name="test_job_0")
        krf_data_0 = KRFJob(krf_json_data_0, decorator_data=decor_data)

        # crete krf data (job-1)
        krf_json_data_1 = create_krf(time_series_2proc, creation_date="2017-07-31T01:28:44+00:00")
        decor_data = KRFDecorator(workload_name="workload_1/something_else/parallel", job_name="test_job_1")
        krf_data_1 = KRFJob(krf_json_data_1, decorator_data=decor_data)

        # instantiate the sim_data object
        sim_data = SimulationData(jobs=[krf_data_0, krf_data_1], sim_name="dummy_sim", n_procs_node=36)

        # test total runtime
        # 11 = 9 + 2 (9 is the total duration of cpu2 and job2 has started 2 seconds after job 1)
        self.assertEqual(sim_data.runtime(), 11)

        # test tmin_epochs
        self.assertEqual(sim_data.tmin_epochs, min(krf_data_0.t_start, krf_data_1.t_start))

        # test tmax_epochs
        self.assertEqual(sim_data.tmax_epochs, max(krf_data_0.t_end, krf_data_1.t_end))

    def test_global_time_series(self):
        """
        Test global time series
        :return:
        """

        time_series_2proc = [
            {
                "flops": np.asarray([0, 111, 0, 111]),
                "bytes_read": np.asarray([333.3, 0, 0, 1]),
                "n_read": np.asarray([3, 0, 0, 1]),
                "bytes_write": np.asarray([0, 444.4, 0, 1]),
                "n_write": np.asarray([0, 2, 0, 1]),
                "durations": np.asarray([1.0, 1.0, 1.0, 1.0])
            },
            {
                "flops": np.asarray([222, 0, 0, 222]),
                "bytes_read": np.asarray([0, 0, 0, 1]),
                "n_read": np.asarray([0, 0, 0, 1]),
                "bytes_write": np.asarray([0, 0, 0, 1]),
                "n_write": np.asarray([0, 0, 0, 1]),
                "durations": np.asarray([1.0, 1.0, 1.0, 6.0])
            }
        ]

        # total float values and time stamps

        # job 0:
        # p0_times  = [1.0, 1.0, 1.0, 1.0 ]
        # p0_values = [0,   111,   0, 111 ]
        # p1_times  = [1.0, 1.0, 1.0, 6.0 ]
        # p1_values = [222,   0,   0, 222 ]

        # ===> job0:
        # times = [1.0, 2.0, 4.0, 9.0]
        # float = [222, 111, 111, 222]
        # ---------------------------------

        # ===> job1:
        # times = [3.0, 4.0, 6.0, 11.0]
        # float = [222, 111, 111, 222]
        # ---------------------------------

        # Simulation totals:
        # times = [1.0, 2.0, 3.0, 4.0, 6.0, 9.0, 11.]
        # float = [222, 111, 222, 222, 111, 222, 222]

        # binned on t_bin_ends = [4.0, 8.0, 12.]
        # tt = [4.0, 8.0, 12.]
        # vv = [777, 333, 222]

        # crete krf data (job-0)
        krf_json_data_0 = create_krf(time_series_2proc, creation_date="2017-07-31T01:28:42+00:00")
        decor_data = KRFDecorator(workload_name="workload_1/something_else/parallel", job_name="test_job_0")
        krf_data_0 = KRFJob(krf_json_data_0, decorator_data=decor_data)

        # crete krf data (job-1)
        krf_json_data_1 = create_krf(time_series_2proc, creation_date="2017-07-31T01:28:44+00:00")
        decor_data = KRFDecorator(workload_name="workload_1/something_else/parallel", job_name="test_job_1")
        krf_data_1 = KRFJob(krf_json_data_1, decorator_data=decor_data)

        # instantiate the sim_data object
        sim_data = SimulationData(jobs=[krf_data_0, krf_data_1], sim_name="dummy_sim", n_procs_node=36)

        # check global time series
        times = [4.0, 8.0, 12.]
        found, calculated_time_series = sim_data.create_global_time_series(times)

        # check that indeed has found jobs that fall in this time range
        self.assertGreater(found, 0)

        # check of the binned values (see above)
        self.assertEqual([v for v in calculated_time_series["flops"].values], [777, 333, 222])

        # check that the integral is conserved
        self.assertEqual(sum([v for v in calculated_time_series["flops"].values]), sum([sum(job.time_series["flops"]["values"]) for job in sim_data.jobs]))

if __name__ == "__main__":
    unittest.main()

