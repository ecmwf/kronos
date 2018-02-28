#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest
from datetime import datetime

import numpy as np
from kronos.shared_tools.shared_utils import datetime2epochs
from kronos.io.format_data_handlers.kresults_job import KResultsJob
from kronos.core.post_process.tests.test_utils import create_kresults
from kronos.io.format_data_handlers.kresults_decorator import KResultsDecorator


class KResultsJobTest(unittest.TestCase):

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

    def test_kresults_basics(self):
        """
        Test main properties of a KResults job
        :return:
        """

        # crete kresults data..
        kresults_json_data = create_kresults(self.time_series_2proc)

        decor_data = KResultsDecorator(workload_name="workload_1/something_else/parallel", job_name="test_job_1")
        kresults_data = KResultsJob(kresults_json_data, decorator_data=decor_data)

        # time start
        self.assertEqual(datetime2epochs( datetime.strptime(kresults_json_data["created"], '%Y-%m-%dT%H:%M:%S+00:00') ), kresults_data.t_end)

        # assert time end
        self.assertEqual(kresults_data.t_start + max([sum(ts["durations"]) for ts in self.time_series_2proc]), kresults_data.t_end)

        # assert duration
        self.assertEqual(kresults_data.t_end-kresults_data.t_start, kresults_data.duration)

        # assert ncpu
        self.assertEqual(kresults_data.n_cpu, len(kresults_json_data["ranks"]))

        # is in class
        self.assertTrue( kresults_data.is_in_class("workload_1/*/parallel"))

    def test_kresults_time_series(self):
        """
        Test KResults job time series
        :return:
        """

        # "flops":     np.asarray([0,   111, 0,   111]),
        # "durations": np.asarray([1.0, 1.0, 1.0, 1.0])

        # "flops":     np.asarray([222, 0,   0,   222]),
        # "durations": np.asarray([1.0, 1.0, 1.0, 6.0])

        # crete kresults data..
        kresults_json_data = create_kresults(self.time_series_2proc)
        decor_data = KResultsDecorator(workload_name="workload-1", job_name="test_job_1")
        kresults_data = KResultsJob(kresults_json_data, decorator_data=decor_data)

        # job time series
        time_series = kresults_data.calc_time_series()

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

