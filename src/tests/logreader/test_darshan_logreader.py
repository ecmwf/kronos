#!/usr/bin/env python
import unittest

from logreader.darshan import DarshanIngestedJobFile

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




if __name__ == "__main__":
    unittest.main()
