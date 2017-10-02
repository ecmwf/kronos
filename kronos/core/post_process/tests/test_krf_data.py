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
from datetime import datetime

from kronos.core.post_process.definitions import datetime2epochs
from kronos.core.post_process.krf_data import KRFJob
from kronos.core.post_process.krf_decorator import KRFDecorator
from kronos.core.post_process.tests.test_utils import create_krf


class KRFJobTest(unittest.TestCase):

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

    def test_krf_basics(self):
        """
        Test main properties of a KRF job
        :return:
        """

        # crete krf data..
        krf_json_data = create_krf(self.time_series_2proc)

        decor_data = KRFDecorator(workload_name="workload_1/something_else/parallel", job_name="test_job_1")
        krf_data = KRFJob(krf_json_data, decorator_data=decor_data)

        # time start
        self.assertEqual(datetime2epochs( datetime.strptime(krf_json_data["created"], '%Y-%m-%dT%H:%M:%S+00:00') ), krf_data.t_start)

        # assert time end
        self.assertEqual(krf_data.t_start + max([sum(ts["durations"]) for ts in self.time_series_2proc]),
                         krf_data.t_end)

        # assert duration
        self.assertEqual(krf_data.t_end-krf_data.t_start, krf_data.duration)

        # assert ncpu
        self.assertEqual(krf_data.n_cpu, len(krf_json_data["ranks"]))

        # is in class
        self.assertTrue( krf_data.is_in_class(("workload_1/*", "parallel")) )

    def test_krf_time_series(self):
        """
        Test KRF job time series
        :return:
        """

        # "flops":     np.asarray([0,   111, 0,   111]),
        # "durations": np.asarray([1.0, 1.0, 1.0, 1.0])

        # "flops":     np.asarray([222, 0,   0,   222]),
        # "durations": np.asarray([1.0, 1.0, 1.0, 6.0])

        # crete krf data..
        krf_json_data = create_krf(self.time_series_2proc)
        decor_data = KRFDecorator(workload_name="workload-1", job_name="test_job_1")
        krf_data = KRFJob(krf_json_data, decorator_data=decor_data)

        # job time series
        time_series = krf_data.calc_time_series()

        # check flops time series (time stamps)
        self.assertEqual(time_series["flops"]["times"], [1.0, 2.0, 4.0, 9.0])

        # check flops time series (values)
        self.assertEqual(time_series["flops"]["values"], [222, 111, 111, 222])

        # check flops time series (elapsed)
        self.assertEqual(time_series["flops"]["elapsed"], [1.0, 1.0, 1.0, 6.0])

        # check flops time series (ratios)
        self.assertEqual(time_series["flops"]["ratios"], [222/1.0, 111/1.0, 111/1.0, 222/6.0])

if __name__ == "__main__":
    unittest.main()

