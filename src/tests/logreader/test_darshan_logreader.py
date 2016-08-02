#!/usr/bin/env python
import unittest

from logreader.darshan import DarshanIngestedJobFile, DarshanIngestedJob


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
        self.assertIsNone(f.read_time)
        self.assertIsNone(f.write_time)

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
        f1.read_time = 123456
        f2.read_time = 456789
        f1.write_time = 222
        f2.write_time = 111
        f1.aggregate(f2)
        self.assertEqual(f1.read_time, 123456)
        self.assertEqual(f1.write_time, 111)

        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time = 456789
        f2.read_time = 123456
        f1.write_time = 111
        f2.write_time = 222
        f1.aggregate(f2)
        self.assertEqual(f1.read_time, 123456)
        self.assertEqual(f1.write_time, 111)

        # One equals None
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time = 123456
        f2.read_time = None
        f1.write_time = None
        f2.write_time = 111
        f1.aggregate(f2)
        self.assertEqual(f1.read_time, 123456)
        self.assertEqual(f1.write_time, 111)

        # The other equals None
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time = None
        f2.read_time = 456789
        f1.write_time = 222
        f2.write_time = None
        f1.aggregate(f2)
        self.assertEqual(f1.read_time, 456789)
        self.assertEqual(f1.write_time, 222)

        # Both equal None
        f1 = DarshanIngestedJobFile("job")
        f2 = DarshanIngestedJobFile("job")
        f1.read_time = None
        f2.read_time = None
        f1.write_time = None
        f2.write_time = None
        f1.aggregate(f2)
        self.assertEqual(f1.read_time, None)
        self.assertEqual(f1.write_time, None)


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

        job1 = DarshanIngestedJob(label="jobA", file_details={
            "file1": file1,
            "file2": file2a
        })

        job2 = DarshanIngestedJob(label="jobA", file_details={
            "file2": file2b,
            "file3": file3
        })

        # We should be able to aggregate the jobs!
        job1.aggregate(job2)
        self.assertEqual(len(job1.file_details), 3)
        self.assertEqual(set(["file1", "file2", "file3"]), set(job1.file_details.keys()))

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

if __name__ == "__main__":
    unittest.main()
