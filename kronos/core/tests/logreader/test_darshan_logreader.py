#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest
import types
import os
import mock

from kronos.core.jobs import IngestedJob
from kronos.core.logreader.base import LogReader
from kronos.core.logreader.darshan import (DarshanIngestedJobFile, DarshanIngestedJob, DarshanLogReaderError, DarshanLogReader,
                               DarshanDataSet)


class DarshanIngestedJobFileTest(unittest.TestCase):
    """
    The DarshanIngestedJobFile class contains the information about each file logged by Darshan, within a job
    """
    def test_initialisation(self):

        # Test that we require a file name/label
        self.assertRaises(TypeError, lambda: DarshanIngestedJobFile())

        f = DarshanIngestedJobFile("a-job-name")
        self.assertEqual(f.name, "a-job-name")

        self.assertEqual(f.bytes_read, 0)
        self.assertEqual(f.bytes_written, 0)
        self.assertEqual(f.read_count, 0)
        self.assertEqual(f.write_count, 0)
        self.assertEqual(f.open_count, 0)
        self.assertIsNone(f.open_time)
        self.assertIsNone(f.read_time_start)
        self.assertIsNone(f.read_time_end)
        self.assertIsNone(f.write_time_start)
        self.assertIsNone(f.write_time_end)
        self.assertIsNone(f.close_time)

    def test_representation(self):

        f = DarshanIngestedJobFile("job")

        f.bytes_read = 123
        f.bytes_written = 456
        f.read_count = 7
        f.write_count = 8

        self.assertEqual(str(f), "DarshanFile(7 reads, 123 bytes, 8 writes, 456 bytes)")
        self.assertEqual(unicode(f), "DarshanFile(7 reads, 123 bytes, 8 writes, 456 bytes)")

    def test_aggregate_same_name(self):
        """
        We can only aggregate records that correspond to the same file!
        """
        f1 = DarshanIngestedJobFile("job1")
        f2 = DarshanIngestedJobFile("job2")

        self.assertRaises(AssertionError, lambda: f1.aggregate(f2))

    def test_aggregate_counters(self):

        f1 = DarshanIngestedJobFile("job")
        f1.bytes_read = 123
        f1.bytes_written = 456
        f1.read_count = 7
        f1.write_count = 8
        f1.open_count = 9

        f2 = DarshanIngestedJobFile("job")
        f2.bytes_read = 901
        f2.bytes_written = 234
        f2.read_count = 5
        f2.write_count = 6
        f2.open_count = 7

        # Aggregate modifies in place, rather than returning anything
        ret = f1.aggregate(f2)
        self.assertIsNone(ret)

        self.assertEqual(f1.bytes_read, 1024)
        self.assertEqual(f1.bytes_written, 690)
        self.assertEqual(f1.read_count, 12)
        self.assertEqual(f1.write_count, 14)
        self.assertEqual(f1.open_count, 16)

    def test_aggregate_times(self):
        """
        For now, the darshan log reader records the _first_ recorded time assocaiated with a file.
        Times are recorded as an integer (unix time)
        """
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time_start = 123456
        f2.read_time_start = 456789
        f1.read_time_end = 444
        f2.read_time_end = 555
        f1.write_time_start = 222
        f2.write_time_start = 111
        f1.write_time_end = 666
        f2.write_time_end = 777
        f1.aggregate(f2)
        self.assertEqual(f1.read_time_start, 123456)
        self.assertEqual(f1.read_time_end, 555)
        self.assertEqual(f1.write_time_start, 111)
        self.assertEqual(f1.write_time_end, 777)

        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time_start = 456789
        f2.read_time_start = 123456
        f1.read_time_end = 555
        f2.read_time_end = 444
        f1.write_time_start = 111
        f2.write_time_start = 222
        f1.write_time_end = 777
        f2.write_time_end = 666
        f1.aggregate(f2)
        self.assertEqual(f1.read_time_start, 123456)
        self.assertEqual(f1.read_time_end, 555)
        self.assertEqual(f1.write_time_start, 111)
        self.assertEqual(f1.write_time_end, 777)

        # One equals None
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time_start = 123456
        f2.read_time_start = None
        f1.read_time_end = 555
        f2.read_time_end = None
        f1.write_time_start = None
        f2.write_time_start = 111
        f1.write_time_end = None
        f2.write_time_end = 666
        f1.aggregate(f2)
        self.assertEqual(f1.read_time_start, 123456)
        self.assertEqual(f1.read_time_end, 555)
        self.assertEqual(f1.write_time_start, 111)
        self.assertEqual(f1.write_time_end, 666)

        # The other equals None
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time_start = None
        f2.read_time_start = 456789
        f1.read_time_end = None
        f2.read_time_end = 444
        f1.write_time_start = 222
        f2.write_time_start = None
        f1.write_time_end = 777
        f2.write_time_end = None
        f1.aggregate(f2)
        self.assertEqual(f1.read_time_start, 456789)
        self.assertEqual(f1.read_time_end, 444)
        self.assertEqual(f1.write_time_start, 222)
        self.assertEqual(f1.write_time_end, 777)

        # Both equal None
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time_start = None
        f2.read_time_start = None
        f1.read_time_end = None
        f2.read_time_end = None
        f1.write_time_start = None
        f2.write_time_start = None
        f1.write_time_end = None
        f2.write_time_end = None
        f1.aggregate(f2)
        self.assertEqual(f1.read_time_start, None)
        self.assertEqual(f1.read_time_end, None)
        self.assertEqual(f1.write_time_start, None)
        self.assertEqual(f1.write_time_end, None)


class DarshanIngestedJobTest(unittest.TestCase):
    """
    The DarshanIngestedJobFile class contains the information about each file logged by Darshan, within a job
    """
    def test_initialisation(self):

        # We require the file details parameter to be fulfilled (even if it with spurious data)
        self.assertRaises(AssertionError, lambda: DarshanIngestedJob())

        # Check defaults
        job = DarshanIngestedJob(file_details={"empty": "job"})
        self.assertEquals(len(job.file_details), 1)
        self.assertEquals(job.file_details['empty'], "job")
        self.assertEqual(job.uid, None)
        self.assertEqual(job.nprocs, None)
        self.assertEqual(job.jobid, None)
        self.assertEqual(job.log_version, None)
        self.assertEqual(job.label, None)

        # We can fill in the details with keyword arguments
        job = DarshanIngestedJob(
            file_details={"another": "attempt"},
            uid=123,
            nprocs=44,
            jobid=56,
            log_version=99.7,
            label="a-label"
        )
        self.assertEquals(len(job.file_details), 1)
        self.assertEquals(job.file_details['another'], "attempt")
        self.assertEqual(job.uid, 123)
        self.assertEqual(job.nprocs, 44)
        self.assertEqual(job.jobid, 56)
        self.assertEqual(job.log_version, 99.7)
        self.assertEqual(job.label, "a-label")

    def test_aggregation_different_jobs(self):
        """
        We should only be able to aggregate two job records that correspond to the same job.
        """
        job1 = DarshanIngestedJob(label="jobA", file_details={})
        job2 = DarshanIngestedJob(label="jobB", file_details={})

        self.assertRaises(AssertionError, lambda: job1.aggregate(job2))

    def test_aggregation(self):
        """
        When we aggregate two jobs, we include all of the file accesses accumulated across all of the
        subcommands that make up the job (each of which will have come from separate files).

        --> The file list should be the union of the two
        --> Data for files that occur in both should be aggregated.
        """
        file1 = DarshanIngestedJobFile(name="file1")
        file2a = DarshanIngestedJobFile(name="file2")
        file2b = DarshanIngestedJobFile(name="file2")
        file3 = DarshanIngestedJobFile(name="file3")

        file1.bytes_read = 123
        file2a.bytes_read = 456
        file2b.bytes_read = 789
        file3.bytes_read = 12

        file1.bytes_written = 345
        file2a.bytes_written = 678
        file2b.bytes_written = 901
        file3.bytes_written = 234

        file1.write_count = 3
        file2a.write_count = 4
        file2b.write_count = 5
        file3.write_count = 6

        file1.read_count = 7
        file2a.read_count = 8
        file2b.read_count = 9
        file3.read_count = 10

        file1.open_count = 1
        file2a.open_count = 2
        file2b.open_count = 3
        file3.open_count = 4

        job1 = DarshanIngestedJob(label="jobA", time_start=123, file_details={
            "file1": file1,
            "file2": file2a
        })

        job2 = DarshanIngestedJob(label="jobA", time_start=456, file_details={
            "file2": file2b,
            "file3": file3
        })

        # We should be able to aggregate the jobs!
        job1.aggregate(job2)
        self.assertEqual(job1.time_start, 123)
        self.assertEqual(len(job1.file_details), 3)
        self.assertEqual({"file1", "file2", "file3"}, set(job1.file_details.keys()))

        f1 = job1.file_details["file1"]
        self.assertEqual(f1.read_count, 7)
        self.assertEqual(f1.write_count, 3)
        self.assertEqual(f1.open_count, 1)
        self.assertEqual(f1.bytes_read, 123)
        self.assertEqual(f1.bytes_written, 345)

        f2 = job1.file_details["file2"]
        self.assertEqual(f2.read_count, 17)
        self.assertEqual(f2.write_count, 9)
        self.assertEqual(f2.open_count, 5)
        self.assertEqual(f2.bytes_read, 1245)
        self.assertEqual(f2.bytes_written, 1579)

        f3 = job1.file_details["file3"]
        self.assertEqual(f3.read_count, 10)
        self.assertEqual(f3.write_count, 6)
        self.assertEqual(f3.open_count, 4)
        self.assertEqual(f3.bytes_read, 12)
        self.assertEqual(f3.bytes_written, 234)

    def test_model_job(self):
        """
        Test that we capture the totals correctly.
        """
        # Log versions > 2 are required
        job = DarshanIngestedJob(
            label="job",
            file_details={},
            log_version="0.1"
        )
        self.assertRaises(DarshanLogReaderError, lambda: job.model_job())

        # If there are no files associated with the job, then the totals should all be zeros.
        job1 = DarshanIngestedJob(
            label="jobA",
            log_version="2.6",
            file_details={},
            time_start=99,
            time_end=123,
            nprocs=17,
            jobid=666,
            uid=777
        )

        m = job1.model_job()

        self.assertEqual(m.label, "jobA")
        self.assertEqual(m.time_start, 99)
        self.assertEqual(m.ncpus, 17)
        self.assertIsNone(m.timesignals['kb_read'])
        self.assertIsNone(m.timesignals['kb_write'])

        # Otherwise, there should be time-series created with the correct totals
        file1 = DarshanIngestedJobFile(name="file1")
        file2 = DarshanIngestedJobFile(name="file2")

        file1.bytes_read = 1024 * 99
        file2.bytes_read = 1024 * 100
        file1.bytes_written = 1024 * 101
        file2.bytes_written = 1024 * 102
        file1.read_time_start = 11.2
        file1.read_time_end = 13.2
        file2.read_time_start = 13.5
        file2.read_time_end = 15.0
        file1.write_time_start = 19.2
        file1.write_time_end = 21.3
        file2.write_time_start = 32
        file2.write_time_end = 35

        job.file_details = {"file1": file1, "file2": file2}

        job1 = DarshanIngestedJob(
            label="jobB",
            log_version="2.6",
            file_details={"file1": file1, "file2": file2},
            time_start=99,
            time_end=123,
            nprocs=17,
            jobid=666,
            uid=777
        )

        m = job1.model_job()

        self.assertEqual(m.label, "jobB")
        self.assertEqual(m.timesignals['kb_read'].sum, 199)
        self.assertEqual(m.timesignals['kb_write'].sum, 203)

    def test_model_time_series(self):
        """
        For now we only consider the totals, and consider them to be offset by "zero" from the start.
        """
        job = DarshanIngestedJob(label="job", file_details={})
        job.time_start = 0

        # With no file data, we should end up with empty time series
        series = job.model_time_series()
        self.assertEqual(len(series), 0)

        # Otherwise, there should be time-series created with the correct totals

        file1 = DarshanIngestedJobFile(name="file1")
        file2 = DarshanIngestedJobFile(name="file2")

        file1.bytes_read = 1024 * 99
        file2.bytes_read = 1024 * 100
        file1.read_time_start = 11.2
        file1.read_time_end = 13.2
        file2.read_time_start = 13.5
        file2.read_time_end = 15.0

        file1.bytes_written = 1024 * 101
        file2.bytes_written = 1024 * 102
        file1.write_time_start = 19.2
        file1.write_time_end = 21.3
        file2.write_time_start = 32
        file2.write_time_end = 35

        job.file_details = {"file1": file1, "file2": file2}

        series = job.model_time_series()
        self.assertIn('kb_read', series)
        self.assertIn('kb_write', series)
        reads = series['kb_read']
        writes = series['kb_write']

        self.assertEqual(reads.sum, 199)
        self.assertEqual(writes.sum, 203)
        self.assertEqual(len(reads.xvalues), 2)
        self.assertEqual(len(writes.xvalues), 2)
        self.assertEqual(len(reads.yvalues), 2)
        self.assertEqual(len(writes.yvalues), 2)
        self.assertEqual(set(reads.xvalues), {11.2, 13.5})
        self.assertEqual(set(writes.xvalues), {19.2, 32})
        self.assertEqual(set(reads.yvalues), {99.0, 100.0})
        self.assertEqual(set(writes.yvalues), {101, 102})


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


class DarshanLogReaderTest(unittest.TestCase):

    def test_initialisation(self):
        """
        Check that we have sensible (overridable) defaults. N.b. inherits from LogReader
        """
        # With standard defaults
        lr = DarshanLogReader('test-path')

        self.assertEqual(lr.parser_command, 'darshan-parser')
        self.assertEqual(lr.path, 'test-path')
        self.assertEqual(lr.file_pattern, "*.gz")
        self.assertTrue(lr.recursive)
        self.assertEqual(lr.label_method, 'directory')
        self.assertEqual(lr.job_class, DarshanIngestedJob)
        self.assertEqual(lr.log_type_name, 'Darshan')
        self.assertEqual(lr.pool_readers, 10)
        self.assertIsInstance(lr, LogReader)

        # And these are overridable?
        lr = DarshanLogReader('test-path', parser='a-parser')

        self.assertEqual(lr.parser_command, 'a-parser')

    def test_read_logs(self):
        """
        Should return a generator of the output of read_log depending on the results of logfiles.

        For Darshan, we get multiple logfiles per job (as one is produces per command that is contained in the
        submit script). Assume that thee are grouped per directory, and aggregate is called on the job class.
        """
        class LogReaderLocal(DarshanLogReader):
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

    def test_read_parser_output(self):
        """
        Given some (correct) output from darshan-parser, do we interpret it properly?
        """
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'parsed-darshan-log'), 'r') as f:
            data = f.read()

        lr = DarshanLogReader('test-path')

        ingested = lr._read_log_internal(data, 'a-file', 'a-label')

        # We return an array of jobs, which in this case will contain only one
        self.assertIsInstance(ingested, list)
        self.assertEqual(len(ingested), 1)
        ingested = ingested[0]
        self.assertIsInstance(ingested, DarshanIngestedJob)

        self.assertEqual(ingested.filename, "a-file")
        self.assertEqual(ingested.jobid, 0)
        self.assertEqual(ingested.label, "a-label")
        self.assertEqual(ingested.log_version, "2.06")
        self.assertEqual(ingested.time_start, 1469134177)
        self.assertEqual(ingested.uid, 1801)

        # We should have parsed the file details correctly
        self.assertEqual(len(ingested.file_details), 3)

        self.assertIn("dummy-path", ingested.file_details)
        f = ingested.file_details["dummy-path"]
        self.assertEqual(f.name, "dummy-path")
        self.assertEqual(f.open_count, 1)
        self.assertEqual(f.bytes_read, 0)
        self.assertEqual(f.bytes_written, 0)
        self.assertEqual(f.read_count, 0)
        self.assertEqual(f.write_count, 0)

        self.assertIn("dummy-path-1", ingested.file_details)
        f = ingested.file_details["dummy-path-1"]
        self.assertEqual(f.name, "dummy-path-1")
        self.assertEqual(f.open_count, 1)
        self.assertEqual(f.bytes_read, 0)
        self.assertEqual(f.bytes_written, 35)
        self.assertEqual(f.read_count, 0)
        self.assertEqual(f.write_count, 2)

        self.assertIn("dummy-path-2", ingested.file_details)
        f = ingested.file_details["dummy-path-2"]
        self.assertEqual(f.name, "dummy-path-2")
        self.assertEqual(f.open_count, 1)
        self.assertEqual(f.bytes_read, 0)
        self.assertEqual(f.bytes_written, 0)
        self.assertEqual(f.read_count, 0)
        self.assertEqual(f.write_count, 0)


class DarshanDataSetTest(unittest.TestCase):

    def test_initialisation(self):

        ds = DarshanDataSet([1, 2, 3], 'a-path', {"A": "Config"})

        self.assertEqual(ds.log_reader_class, DarshanLogReader)
        self.assertEqual(ds.joblist, [1, 2, 3])
        self.assertEqual(ds.ingest_path, 'a-path')
        self.assertEqual(ds.ingest_config, {"A": "Config"})

    def test_model_job_start_time(self):
        """
        Check that:
          i) The DarshanDataSet correctly extracts the earliest time as the start time
          ii) THis is passed to the job's model_job routine (and the result returned)
        """

        class FakeJob(IngestedJob):
            def __init__(self, time):
                super(FakeJob, self).__init__()
                self.time_start = time
                self.model_job = mock.Mock()
                self.model_job.return_value = 123

        jobs = [FakeJob(5), FakeJob(10), FakeJob(15)]

        ds = DarshanDataSet(jobs, "a-path", {})

        [self.assertEqual(j.model_job.call_count, 0) for j in jobs]
        results = list(ds.model_jobs())
        # [self.assertEqual(j.model_job.call_count, 1) for j in jobs]
        # [self.assertEqual(j.model_job.call_args[0][0], 5) for j in jobs]
        [self.assertEqual(r, 123) for r in results]


if __name__ == "__main__":
    unittest.main()
