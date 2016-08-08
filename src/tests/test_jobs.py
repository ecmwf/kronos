#!/usr/bin/env python
import unittest

from exceptions_iows import ModellingError
from jobs import ModelJob
import time_signal


class ModelJobTest(unittest.TestCase):

    def test_initialisation(self):

        # Test some defaults
        job = ModelJob()

        for attr in ['time_start', 'ncpus', 'nnodes', 'duration', 'label']:
            self.assertTrue(hasattr(job, attr))
            self.assertIsNone(getattr(job, attr))

        for ts_name in time_signal.signal_types:
            self.assertIn(ts_name, job.timesignals)
            self.assertIsNone(job.timesignals[ts_name])

        # Test that we can override specified fields
        job = ModelJob(
            time_series={
                'kb_read': time_signal.TimeSignal.from_values('kb_read', [0.0], [0.0]),
                'kb_write': time_signal.TimeSignal.from_values('kb_write', [0.0], [0.0]),
            },
            time_start=123,
            ncpus=4,
            nnodes=5,
            duration=678,
            label="a-label")

        self.assertEqual(job.time_start, 123)
        self.assertEqual(job.ncpus, 4)
        self.assertEqual(job.nnodes, 5)
        self.assertEqual(job.duration, 678)
        self.assertEqual(job.label, "a-label")
        self.assertIsInstance(job.timesignals['kb_read'], time_signal.TimeSignal)
        self.assertIsInstance(job.timesignals['kb_write'], time_signal.TimeSignal)

        for ts_name in time_signal.signal_types:
            if ts_name in ['kb_read', 'kb_write']:
                continue
            self.assertIn(ts_name, job.timesignals)
            self.assertIsNone(job.timesignals[ts_name])

        # Test that we cannot override non-specific fields
        self.assertRaises(AssertionError, lambda: ModelJob(invalid=123))

    def test_reject_mislabelled_time_signals(self):
        """
        The initialisation routine should reject invalid time signals in a model job.
        """
        self.assertRaises(
            ModellingError,
            lambda: ModelJob(
                time_series={
                    'kb_write': time_signal.TimeSignal.from_values('kb_read', [0.0], [0.0]),
                }))

    def test_merge_fails_different_label(self):
        """
        We should not be able to merge two jobs with differing labels, as these don't correspond to the same overall job
        """
        job1 = ModelJob(label="a-label-1")
        job2 = ModelJob(label="a-label-2")

        self.assertRaises(AssertionError, lambda: job1.merge(job2))

    def test_merge_ignores_empty_timesignals(self):
        """
        When merging in time signals from another job, if there is no data in the "other" time signal, then it
        should be ignored for merging purposes.
        :return:
        """
        kb_read = time_signal.TimeSignal.from_values('kb_read', [0.0], [1.0])
        kb_write = time_signal.TimeSignal.from_values('kb_write', [0.0], [0.0])  # n.b. zero data

        job1 = ModelJob(label="label1", time_series={'kb_read': kb_read})
        job2 = ModelJob(label="label1", time_series={'kb_write': kb_write})

        self.assertIsNone(job1.timesignals['kb_write'])
        self.assertIsNotNone(job2.timesignals['kb_write'])
        job1.merge(job2)
        self.assertIsNone(job1.timesignals['kb_write'])

    def test_merge_rejects_mislabelled_time_signals(self):
        """
        Test that the merging routine checks the labelling validity. Both ways around.
        :return:
        """
        kb_read = time_signal.TimeSignal.from_values('kb_read', [0.0], [1.0])
        kb_write = time_signal.TimeSignal.from_values('kb_read', [0.0], [1.0])  # n.b. mislabelled

        job1 = ModelJob(label="label1", time_series={'kb_read': kb_read})
        job2 = ModelJob(label="label1")
        job2.timesignals['kb_write'] = kb_write

        self.assertRaises(ModellingError, lambda: job1.merge(job2))

        # And the other way around
        job2 = ModelJob(label="label1", time_series={'kb_read': kb_read})
        job1 = ModelJob(label="label1")
        job1.timesignals['kb_write'] = kb_write

        self.assertRaises(ModellingError, lambda: job1.merge(job2))

    def test_merge(self):
        """
        Can we merge multiple ModelJobs.

        n.b. Currently this only supports time signals!
        TODO: Merge the non-time-signal data.
        """
        # n.b. non-zero values. Zero time signals are ignored.
        kb_read1 = time_signal.TimeSignal.from_values('kb_read', [0.0], [1.0])
        kb_read2 = time_signal.TimeSignal.from_values('kb_read', [0.0], [1.0])
        kb_write1 = time_signal.TimeSignal.from_values('kb_write', [0.0], [1.0])

        # Test that we take the union of the available time series
        job1 = ModelJob(label="label1", time_series={'kb_read': kb_read1})
        job2 = ModelJob(label="label1", time_series={'kb_write': kb_write1})
        job1.merge(job2)

        self.assertEqual(len(job1.timesignals), len(time_signal.signal_types))
        self.assertEqual(job1.timesignals['kb_read'], kb_read1)
        self.assertEqual(job1.timesignals['kb_write'], kb_write1)

        # (The other time signals should still be None)
        for ts_name in time_signal.signal_types:
            if ts_name in ['kb_read', 'kb_write']:
                continue
            self.assertIn(ts_name, job1.timesignals)
            self.assertIsNone(job1.timesignals[ts_name])

        # If a time-series exists in BOTH jobs this should (for now) raise an error.
        job1 = ModelJob(label="label1", time_series={'kb_read': kb_read1})
        job2 = ModelJob(label="label1", time_series={'kb_read': kb_read2})
        self.assertRaises(ModellingError, lambda: job1.merge(job2))

    def test_is_valid(self):
        """
        There are some things that are required. Check these things here!
        """
        job = ModelJob()
        self.assertFalse(job.is_valid())

        # If all of the required arguments are supplied, this should result in a valid job
        valid_args = {
            'time_start': 0,
            'ncpus': 1,
            'nnodes': 1
        }
        self.assertTrue(ModelJob(**valid_args).is_valid())

        # If any of the supplied arguments are missing, this should invalidate things
        for k in valid_args.keys():
            invalid_args = valid_args.copy()
            del invalid_args[k]
            self.assertTrue(ModelJob(**valid_args).is_valid())


class IngestedJobTest(unittest.TestCase):
    """
    Note that the functionality of IngestedJob base class is too broad as it stands. This should be highly redacted
    once the input parsers have all been migrated to the LogReader formulation.
    """
    def test_initialisation(self):
        """
        Check that we have sensible (overridable) defaults. N.b. inherits from LogReader
        """
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
