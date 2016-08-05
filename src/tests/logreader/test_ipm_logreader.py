#!/usr/bin/env python
import unittest
import types

from logreader.ipm import IPMTaskInfo, IPMLogReader


class IPMTaskInfoTest(unittest.TestCase):
    """
    The IPMTaskInfo class contains the information about each task logged by IPM within a job
    """
    available_attrs = [
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
        task = IPMTaskInfo()

        for attr in self.available_attrs:
            self.assertTrue(hasattr(task, attr))
            self.assertEqual(getattr(task, attr), 0)

    def test_only_attrs(self):
        """
        Test that we haven't added any further attributes that aren't tested.
        """
        task = IPMTaskInfo()

        task_attrs = [a for a in dir(task) if not (a.startswith('_') or callable(getattr(task, a)))]

        for attr in task_attrs:
            self.assertIn(attr, self.available_attrs)

    def test_representation(self):
        """
        There are too many data elements to represent them all, so test that we record some sensible aggregates.
        """
        task = IPMTaskInfo()

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
    def test_initialisation(self):

        self.assertFalse(True)


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
        self.assertFalse(True)

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


if __name__ == "__main__":
    unittest.main()
