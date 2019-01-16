# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import unittest
from datetime import datetime

from kronos_executor.io_formats.format_data_handlers.tests.local_test_utils import create_kresults
from kronos_executor.tools import datetime2epochs
from kronos_executor.io_formats.format_data_handlers.kresults_decorator import KResultsDecorator
from kronos_executor.io_formats.format_data_handlers.kresults_job import KResultsJob


class KResultsJobsTest(unittest.TestCase):

    # example of serial job
    kresult_job_0 = {
                        "created": "2018-02-26T15:34:44+00:00",
                        "kronosSHA1": "",
                        "kronosVersion": "0.1.4",
                        "ranks": [
                            {
                                "host": "lxg04",
                                "pid": 30481,
                                "rank": 0,
                                "stats": {
                                    "cpu": {
                                        "averageElapsed": 2.575281e-09,
                                        "count": 10000000000,
                                        "elapsed": 25.75281,
                                        "stddevElapsed": 0.0002575281,
                                        "sumSquaredElapsed": 663.2071
                                    },
                                    "write": {
                                        "averageBytes": 256000,
                                        "averageElapsed": 0.0003455162,
                                        "bytes": 2560000,
                                        "count": 10,
                                        "elapsed": 0.003455162,
                                        "stddevBytes": 0,
                                        "stddevElapsed": 0.0003220533,
                                        "sumSquaredBytes": 655360000000,
                                        "sumSquaredElapsed": 2.230997e-06
                                    }
                                },
                                "time_series": {
                                    "bytes_write": [
                                        0,
                                        2560000
                                    ],
                                    "durations": [
                                        25.75281,
                                        0.007616043
                                    ],
                                    "flops": [
                                        10000000000,
                                        0
                                    ],
                                    "n_write": [
                                        0,
                                        10
                                    ]
                                }
                            }
                        ],
                        "tag": "KRONOS-KRESULTS-MAGIC",
                        "uid": 4426,
                        "version": 1
                    }

    # example of parallel job with 2 processors
    kresult_job_1 = {
                        "created": "2018-02-26T15:35:36+00:00",
                        "kronosSHA1": "",
                        "kronosVersion": "0.1.4",
                        "ranks": [
                            {
                                "host": "lxg04",
                                "pid": 30583,
                                "rank": 0,
                                "stats": {
                                    "cpu": {
                                        "averageElapsed": 0.03,
                                        "count": 999,
                                        "elapsed": 999/0.03,
                                        "stddevElapsed": 0.0,
                                        "sumSquaredElapsed": sum([0.03**2]*999)
                                    }
                                },
                                "time_series": {
                                    "durations": [
                                        0.03*333,
                                        0.03*666
                                    ],
                                    "flops": [
                                        333,
                                        666
                                    ]
                                }
                            },
                            {
                                "host": "lxg04",
                                "pid": 30584,
                                "rank": 1,
                                "stats": {
                                    "cpu": {
                                        "averageElapsed": 0.01,
                                        "count": 30,
                                        "elapsed": 30/0.01,
                                        "stddevElapsed": 0.0,
                                        "sumSquaredElapsed": sum([0.01**2]*30)
                                    }
                                },
                                "time_series": {
                                    "durations": [
                                        0.01*10,
                                        0.01*20
                                    ],
                                    "flops": [
                                        10,
                                        20
                                    ]
                                }
                            }
                        ],
                        "tag": "KRONOS-KRESULTS-MAGIC",
                        "uid": 4426,
                        "version": 1
                    }

    # user-defined time series to be used to make synthetic kresult files..
    time_series_2proc = [
        {
            "flops": [0, 111, 0, 111],
            "bytes_read": [333.3, 0, 0, 1],
            "n_read": [3, 0, 0, 1],
            "bytes_write": [0, 444.4, 0, 1],
            "n_write": [0, 2, 0, 1],
            "durations": [1.0, 1.0, 1.0, 1.0]
        },
        {
            "flops": [222, 0, 0, 222],
            "bytes_read": [0, 0, 0, 1],
            "n_read": [0, 0, 0, 1],
            "bytes_write": [0, 0, 0, 1],
            "n_write": [0, 0, 0, 1],
            "durations": [1.0, 1.0, 1.0, 6.0]
        }
    ]

    def test_serial_job_info(self):

        # serial job
        job0 = KResultsJob(self.kresult_job_0)
        self.assertEqual(job0.name, None)
        self.assertEqual(job0.label, None)
        self.assertEqual(job0.n_cpu, 1)
        self.assertEqual(job0.get_class_name({}), ["generic_class"])
        self.assertEqual(job0.is_in_class("dummy_class"), False)

    def test_parallel_job_info(self):

        # parallel job
        job1 = KResultsJob(self.kresult_job_1)
        self.assertEqual(job1.name, None)
        self.assertEqual(job1.label, None)
        self.assertEqual(job1.n_cpu, 2)
        self.assertEqual(job1.get_class_name({}), ["generic_class"])
        self.assertEqual(job1.is_in_class("dummy_class"), False)

    def test_serial_job_timings(self):

        # serial job
        job0 = KResultsJob(self.kresult_job_0)
        end_datetime_job1 = datetime2epochs(datetime.strptime(job0._json_data["created"], '%Y-%m-%dT%H:%M:%S+00:00'))
        self.assertEqual(job0.t_end, end_datetime_job1)
        self.assertEqual(job0.duration, 25.75281+0.007616043)
        self.assertEqual(job0.t_start, end_datetime_job1-(25.75281+0.007616043))

    def test_parallel_job_timings(self):

        # parallel job
        job1 = KResultsJob(self.kresult_job_1)
        end_datetime_job1 = datetime2epochs(datetime.strptime(job1._json_data["created"], '%Y-%m-%dT%H:%M:%S+00:00'))
        self.assertEqual(job1.t_end, end_datetime_job1)
        self.assertEqual(job1.duration, 0.03*333+0.03*666)
        self.assertEqual(job1.t_start, end_datetime_job1-(0.03*333+0.03*666))

    def test_serial_job_time_series(self):

        # serial job
        job0 = KResultsJob(self.kresult_job_0)

        _ts = job0.calc_time_series()
        self.assertEqual(_ts["flops"]["times"], [25.75281])
        self.assertEqual(_ts["kb_write"]["times"], [25.75281 + 0.007616043])
        self.assertEqual(_ts["n_write"]["times"], [25.75281 + 0.007616043])

        self.assertEqual(_ts["flops"]["values"], [10000000000])
        self.assertEqual(_ts["kb_write"]["values"], [2560000/1024.0])
        self.assertEqual(_ts["n_write"]["values"], [10])

        self.assertEqual(_ts["flops"]["ratios"], [10000000000/25.75281])
        self.assertEqual(_ts["kb_write"]["ratios"], [2560000/1024.0/0.007616043])
        self.assertEqual(_ts["n_write"]["ratios"], [10/0.007616043])

        self.assertEqual(_ts["flops"]["elapsed"], [25.75281])
        self.assertEqual(_ts["kb_write"]["elapsed"], [0.007616043])
        self.assertEqual(_ts["n_write"]["elapsed"], [0.007616043])

    def test_parallel_job_time_series(self):

        # serial job
        job1 = KResultsJob(self.kresult_job_1)

        _ts = job1.calc_time_series()
        self.assertEqual(_ts["flops"]["times"], [0.01*10, 0.01*10+0.01*20, 0.03*333, 0.03*333+0.03*666])
        self.assertEqual(_ts["flops"]["values"], [10, 20, 333, 666])
        self.assertEqual(_ts["flops"]["ratios"], [10/(0.01*10), 20/(0.01*20), 333/(0.03*333), 666/(0.03*666)])
        self.assertEqual(_ts["flops"]["elapsed"], [0.01*10, 0.01*20, 0.03*333, 0.03*666])

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
        self.assertTrue( kresults_data.is_in_class("^workload_1/.*/parallel"))

    def test_kresults_time_series(self):
        """
        Test KResults job time series
        :return:
        """

        # "flops":     [0,   111, 0,   111],
        # "durations": [1.0, 1.0, 1.0, 1.0]

        # "flops":     [222, 0,   0,   222],
        # "durations": [1.0, 1.0, 1.0, 6.0]

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

