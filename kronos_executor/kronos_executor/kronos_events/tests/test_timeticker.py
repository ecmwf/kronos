#!/usr/bin/env python

import unittest
from datetime import datetime
from datetime import timedelta

from kronos_executor.kronos_events.time_ticker import TimeTicker


class TimeTickerTests(unittest.TestCase):

    def test_timeticker(self):

        starttime = datetime.now()
        ticker = TimeTicker(starttime, keep_all_times=True)

        # define some time deltas to try
        sec_list = list(ticker.get_elapsed_seconds(starttime+timedelta(seconds=2.2)))
        self.assertEqual(sec_list, [0, 1, 2])
        self.assertEqual(ticker.all_times, [0, 1, 2])
        self.assertEqual(ticker.last_logged_second, 2)

        sec_list = list(ticker.get_elapsed_seconds(starttime+timedelta(seconds=9.1)))
        self.assertEqual(sec_list, [3, 4, 5, 6, 7, 8, 9])
        self.assertEqual(ticker.all_times, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        self.assertEqual(ticker.last_logged_second, 9)

        sec_list = list(ticker.get_elapsed_seconds(starttime+timedelta(seconds=11)))
        self.assertEqual(sec_list, [10, 11])
        self.assertEqual(ticker.all_times, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
        self.assertEqual(ticker.last_logged_second, 11)

        sec_list = list(ticker.get_elapsed_seconds(starttime+timedelta(seconds=11.9)))
        self.assertEqual(sec_list, [])
        self.assertEqual(ticker.all_times, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
        self.assertEqual(ticker.last_logged_second, 11)


if __name__ == "__main__":

    unittest.main()
