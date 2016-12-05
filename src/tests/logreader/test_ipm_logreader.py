#!/usr/bin/env python
import unittest
import types
import mock

from exceptions_iows import IngestionError
from jobs import IngestedJob
from logreader.base import LogReader
from logreader.ipm import IPMTaskInfo, IPMLogReader, IPMIngestedJob, IPMDataSet


class IPMTaskInfoTest(unittest.TestCase):
    """
    The IPMTaskInfo class contains the information about each task logged by IPM within a job
    """
    task_metadata = [
        'ipm_version',
        'ntasks',
        'nhosts',
        'mpi_rank',
        'time_start',
        'time_end'
    ]

    available_counters = [
        'mpi_pairwise_count_send',
        'mpi_pairwise_bytes_send',
        'mpi_pairwise_count_recv',
        'mpi_pairwise_bytes_recv',

        'mpi_collective_count',
        'mpi_collective_bytes',

        'open_count',
        'read_count',
        'write_count',
        'bytes_read',
        'bytes_written',
    ]

    def test_initialisation(self):
        """
        Test that all the required accumulators are created, and zerod
        """
        task = IPMTaskInfo("2.0.2", 99, 88, 77, 55, 66)

        for attr in self.available_counters:
            self.assertTrue(hasattr(task, attr))
            self.assertEqual(getattr(task, attr), 0)

        self.assertEqual(task.ipm_version, "2.0.2")
        self.assertEqual(task.ntasks, 99)
        self.assertEqual(task.nhosts, 88)
        self.assertEqual(task.mpi_rank, 77)
        self.assertEqual(task.time_start, 55)
        self.assertEqual(task.time_end, 66)

    def test_only_attrs(self):
        """
        Test that we haven't added any further attributes that aren't tested.
        """
        task = IPMTaskInfo("2.0.2", 0, 0, 0, 0, 0)

        task_attrs = [a for a in dir(task) if not (a.startswith('_') or callable(getattr(task, a)))]

        for attr in task_attrs:
            self.assertTrue(attr in self.available_counters or attr in self.task_metadata)

    def test_start_end_times(self):
        """
        If the start time is after the end time, there should be an error
        """
        self.assertRaises(IngestionError, lambda: IPMTaskInfo("2.0.2", 99, 88, 77, 66, 55))

    def test_representation(self):
        """
        There are too many data elements to represent them all, so test that we record some sensible aggregates.
        """
        task = IPMTaskInfo("2.0.2", 0, 0, 0, 0, 0)

        task.mpi_pairwise_count_send = 1
        task.mpi_pairwise_bytes_send = 2
        task.mpi_pairwise_count_recv = 3
        task.mpi_pairwise_bytes_recv = 4
        task.mpi_collective_count = 5
        task.mpi_collective_bytes = 6
        task.open_count = 7
        task.read_count = 8
        task.write_count = 9
        task.bytes_read = 10
        task.bytes_written = 11

        self.assertEqual(str(task), "IPMTaskInfo(9 MPI events, 12 MPI bytes, 24 IO events, 21 IO bytes)")
        self.assertEqual(unicode(task), "IPMTaskInfo(9 MPI events, 12 MPI bytes, 24 IO events, 21 IO bytes)")

class IPMIngestedJobTest(unittest.TestCase):
    """
    The DarshanIngestedJobFile class contains the information about each file logged by Darshan, within a job
    """
    expected_series = [
        'n_collective',
        'kb_collective',
        'n_pairwise',
        'kb_pairwise',
        'kb_read',
        'kb_write',
        'n_read',
        'n_write'
    ]

    def test_initialisation(self):

        # Check defaults
        job = IPMIngestedJob()
        self.assertEqual(job.tasks, [])
        self.assertIsNone(job.label)
        self.assertIsNone(job.filename)

        # Check that the defaults can be correctly overridden
        job = IPMIngestedJob(
            tasks=[1, 2, 3],
            label="a-label",
            filename="a-filename"
        )
        self.assertEqual(job.tasks, [1, 2, 3])
        self.assertEqual(job.label, "a-label")
        self.assertEqual(job.filename, "a-filename")

    def test_aggregation_different_jobs(self):
        """
        We should only be able to aggregate two job records that correspond to the same job
        """
        job1 = IPMIngestedJob(
            tasks=[1, 2, 3],
            label='label1'
        )
        job2 = IPMIngestedJob(
            tasks=[4, 5, 6],
            label='label2'
        )

        self.assertRaises(AssertionError, job1.aggregate(job2))

    def test_aggregation(self):
        """
        The lists of profiled tasks should be aggregated. For now, that is all that is being returned by IPM, but
        clearly the global data will need to be checked (and ensure that there aren't conflicts...).
        """
        job1 = IPMIngestedJob(
            tasks=[1, 2, 3],
            label='label1'
        )
        job2 = IPMIngestedJob(
            tasks=[4, 5, 6],
            label='label1'
        )

        # If jobs can be aggregated, their task lists should be combined
        job1.aggregate(job2)
        for t in range(1, 7):
            self.assertIn(t, job1.tasks)

    def test_model_job(self):
        """
        n.b. We don't test the time series in detail (that is the following test), but we check that they are
             translated correctly into the ModelJob
        """
        # TODO: Is there a required version of IPM for the parsing we are doing?

        task1 = IPMTaskInfo("2.0.2", 99, 88, 77, 55, 66)
        task1.mpi_pairwise_bytes_send = 123 * 1024
        task1.mpi_pairwise_count_send = 12
        job = IPMIngestedJob(label="a-label", tasks=[task1])

        m = job.model_job()

        self.assertEqual(m.label, "a-label")
        for s in self.expected_series:
            self.assertIsNotNone(m.timesignals.get(s, None))
            self.assertEqual(len(m.timesignals[s].xvalues), 1)
            self.assertEqual(len(m.timesignals[s].yvalues), 1)
        self.assertEqual(m.timesignals['kb_pairwise'].sum, 123)
        self.assertEqual(m.timesignals['n_pairwise'].sum, 12)

        self.assertEqual(m.duration, 11)
        self.assertEqual(m.time_start, 55)
        self.assertEqual(m.ncpus, 99)
        self.assertEqual(m.nnodes, 88)

    def test_start_time(self):
        """
        The property time_start should return the earliest start time of any of the tasks
        """
        task1 = IPMTaskInfo("2.0.2", 99, 88, 0, 44, 66)
        task2 = IPMTaskInfo("2.0.2", 101, 91, 1, 55, 77)
        job = IPMIngestedJob(label="a-label", tasks=[task1, task2])

        self.assertEqual(job.time_start, 44)

    def test_model_job_minmax(self):
        """
        What happens if the tasks don't agree on some of the metrics? Sometimes we need the min, sometimes the max.
        """
        task1 = IPMTaskInfo("2.0.2", 99, 88, 0, 44, 66)
        task2 = IPMTaskInfo("2.0.2", 101, 91, 1, 55, 77)
        job = IPMIngestedJob(label="a-label", tasks=[task1, task2])

        m = job.model_job()

        # n.b. Subtracted a global 9 seconds off the start time for the global start time
        self.assertEqual(m.time_start, 44)
        self.assertEqual(m.duration, 33)
        self.assertEqual(m.ncpus, 101)
        self.assertEqual(m.nnodes, 91)

    def test_model_time_series(self):
        """
        For now, we only consider the totals, and consider them to be offset by "zero" from the start.
        """
        # With no data, we should end up with empty time series
        job = IPMIngestedJob()
        series = job.model_time_series()

        self.assertEqual(set(self.expected_series), set(series.keys()))
        for s in series.values():
            self.assertEqual(s.sum, 0)
            self.assertEqual(len(s.xvalues), 1)
            self.assertEqual(len(s.yvalues), 1)
            self.assertEqual(s.xvalues[0], 0.0)
            self.assertEqual(s.yvalues[0], 0.0)

        # Otherwise, there should be time-series created with the correct totals
        # N.B. The MPI pairwise RECV data is ignored (as it duplicates some/most of the SEND data).

        task1 = IPMTaskInfo("2.0.2", 99, 88, 77, 55, 66)
        task1.mpi_pairwise_bytes_send = 123 * 1024
        task1.mpi_pairwise_bytes_recv = 456 * 1024
        task1.mpi_collective_bytes = 789 * 1024
        task1.bytes_read = 12 * 1024
        task1.bytes_written = 345 * 1024

        task1.mpi_pairwise_count_send = 12
        task1.mpi_pairwise_count_recv = 34
        task1.mpi_collective_count = 56
        task1.open_count = 78
        task1.read_count = 90
        task1.write_count = 12

        task2 = IPMTaskInfo("2.0.2", 99, 88, 77, 55, 66)
        task2.mpi_pairwise_bytes_send = 345 * 1024
        task2.mpi_pairwise_bytes_recv = 678 * 1024
        task2.mpi_collective_bytes = 901 * 1024
        task2.bytes_read = 234 * 1024
        task2.bytes_written = 567 * 1024

        task2.mpi_pairwise_count_send = 89
        task2.mpi_pairwise_count_recv = 1
        task2.mpi_collective_count = 23
        task2.open_count = 45
        task2.read_count = 67
        task2.write_count = 89

        job = IPMIngestedJob(tasks=[task1, task2])
        series = job.model_time_series()

        totals = {
            'n_collective': 79,
            'kb_collective': 1690,
            'n_pairwise': 101,
            'kb_pairwise': 468,
            'kb_read': 246,
            'kb_write': 912,
            'n_read': 157,
            'n_write': 101
        }

        self.assertEqual(set(self.expected_series), set(series.keys()))
        for nm, s in series.iteritems():
            self.assertEqual(s.sum, totals[nm])
            self.assertEqual(len(s.xvalues), 1)
            self.assertEqual(len(s.yvalues), 1)
            self.assertEqual(s.xvalues[0], 0.0)
            self.assertEqual(s.yvalues[0], totals[nm])


class FakeJob(object):
    """
    This is a mocked-up ingested job. It only exists as a file-level class so that it is picklable, which
    makes it work happily with the multiprocessing module.
    """
    def __init__(self, name, label):
        self.names = [name]
        self.label = label

    def aggregate(self, other):
        self.names += other.names


class IPMLogReaderTest(unittest.TestCase):

    def test_initialisation(self):
        """
        Check that we have sensible (overridable) defaults. N.b. inherits from LogReader
        """
        # With standard defaults
        lr = IPMLogReader('test-path')

        self.assertEqual(lr.path, 'test-path')
        self.assertEqual(lr.file_pattern, "*.xml")
        self.assertTrue(lr.recursive)
        self.assertEqual(lr.label_method, 'directory')
        self.assertEqual(lr.job_class, IPMIngestedJob)
        self.assertEqual(lr.log_type_name, 'IPM')
        self.assertEqual(lr.pool_readers, 10)
        self.assertIsInstance(lr, LogReader)

        # And these are overridable?
        lr = IPMLogReader(
            'test-path',
            recursive=False,
            file_pattern="blah.*",
            label_method="directory-no-par-serial",
            pool_readers=1)

        self.assertEqual(lr.file_pattern, "blah.*")
        self.assertFalse(lr.recursive)
        self.assertEqual(lr.label_method, 'directory-no-par-serial')
        self.assertEqual(lr.pool_readers, 1)

    def test_read_logs(self):
        """
        Should return a generator of the output of read_log depending on the results of logfiles.

        For IPM, we get multiple logfiles per job (as one is produces per command that is contained in the
        submit script). Assume that thee are grouped per directory, and aggregate is called on the job class.
        """
        class LogReaderLocal(IPMLogReader):
            def logfiles(self):
                for file_path in ['dir1/file1', 'dir1/file2', 'dir1/file3', 'dir2/file4']:
                    yield file_path

            def read_log(self, filename, suggested_label):
                return [FakeJob(filename, suggested_label)]

        lr = LogReaderLocal('test-path')

        logs = lr.read_logs()
        self.assertIsInstance(logs, types.GeneratorType)

        logs = list(logs)
        self.assertEqual(len(logs), 2)

        self.assertEqual(logs[0].names, ['dir1/file1', 'dir1/file2', 'dir1/file3'])
        self.assertEqual(logs[1].names, ['dir2/file4'])


class IPMDataSetTest(unittest.TestCase):

    def test_initialisation(self):

        ds = IPMDataSet([1, 2, 3], 'a-path', {"A": "Config"})

        self.assertEqual(ds.log_reader_class, IPMLogReader)
        self.assertEqual(ds.joblist, [1, 2, 3])
        self.assertEqual(ds.ingest_path, 'a-path')
        self.assertEqual(ds.ingest_config, {"A": "Config"})

    def test_model_job(self):
        """
        Check that that modelling is deferred to the job class
        """

        class FakeJob(IngestedJob):
            def __init__(self, time):
                super(FakeJob, self).__init__()
                self.time_start = time
                self.model_job = mock.Mock()
                self.model_job.return_value = 123

        jobs = [FakeJob(5), FakeJob(10), FakeJob(15)]

        ds = IPMDataSet(jobs, "a-path", {})

        [self.assertEqual(j.model_job.call_count, 0) for j in jobs]
        results = list(ds.model_jobs())
        [self.assertEqual(j.model_job.call_count, 1) for j in jobs]
        # [self.assertEqual(j.model_job.call_args[0][0], 5) for j in jobs]
        [self.assertEqual(r, 123) for r in results]


if __name__ == "__main__":
    unittest.main()
