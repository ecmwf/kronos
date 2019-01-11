#!/usr/bin/env python
# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import copy
import unittest

from kronos.kronos_modeller.time_signal.definitions import time_signal_names
from kronos.kronos_modeller.time_signal.time_signal import TimeSignal

from kronos_modeller.tools import UserGeneratedJob


class UserJobTests(unittest.TestCase):

    xvals = range(10)
    yvals = [y ** 2 for y in range(10)]

    dummy_time_signals = {tsname: TimeSignal.from_values(tsname,
                                                         xvals=range(10),
                                                         yvals=[y ** 2 for y in range(10)],
                                                         priority=10
                                                         ) for tsname in time_signal_names}

    def test_user_job_init(self):
        """
        Test initialisation of user-generated jobs
        :return:
        """

        # instantiate user job
        user_job = UserGeneratedJob("dummy_job", timesignals=self.dummy_time_signals, ts_scales=None)

        self.assertEqual(user_job.name, "dummy_job")
        self.assertEqual(user_job.timesignals, self.dummy_time_signals)

        # from its proto-signals
        job = UserGeneratedJob.from_random_proto_signals("from_proto_signals_job", ts_len=25)

        self.assertEquals(job.name, "from_proto_signals_job")

        first_ts_len = len(job.timesignals.values()[0].xvalues)
        self.assertEquals(first_ts_len, 25)

    def test_timesignal_probability(self):

        # from its proto-signals
        job = UserGeneratedJob.from_random_proto_signals("from_proto_signals_job", ts_len=25)

        # check length of all the ts..
        first_ts_len = len(job.timesignals.values()[0].xvalues)
        self.assertEquals(first_ts_len, 25)

        # check that all the lengths
        for tsv in job.timesignals.values():
            self.assertEquals(len(tsv.xvalues), first_ts_len)

        # probability 0 meant that all the signals will be removed
        job_no_ts = copy.deepcopy(job)
        job_no_ts.apply_ts_probability(0.0)
        for tsv in job_no_ts.timesignals.values():
            self.assertTrue(all([y == -1 for y in tsv.yvalues]))

        # probability 1 meant that all the signals are retained
        job_no_ts = copy.deepcopy(job)
        job_no_ts.apply_ts_probability(1.0)
        for tsv in job_no_ts.timesignals.values():
            self.assertTrue(all([y != -1 for y in tsv.yvalues]))


if __name__ == "__main__":

    unittest.main()
