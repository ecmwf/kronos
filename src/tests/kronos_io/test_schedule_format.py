#!/usr/bin/env python

from kronos_io.schedule_format import ScheduleFormat
import time_signal

from StringIO import StringIO
from datetime import datetime
import jsonschema
import unittest
import json
import copy


class ScheduleFormatTest(unittest.TestCase):

    def test_schema(self):
        """
        Check that the schema method is correctly loading something that looks schema-ish
        """
        s = ScheduleFormat.schema()

        self.assertIsInstance(s, dict)
        self.assertEqual(s['$schema'], 'http://json-schema.org/draft-03/schema')
        self.assertIn("type", s)
        self.assertIn("properties", s)
        self.assertIn("version", s['properties'])

    def test_validate(self):
        """
        Although validation is normally done internally to the class (when supplied with a file like object,
        etc.), it is actually done on a dictionary. Check that the validation itself is actually working.
        """
        valid = {
            "version": 1,
            "tag": "KRONOS-KSF-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
        }

        # Check the validation information

        invalid = valid.copy()
        invalid['tag'] = "wrong-tag"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['version'] = "-1"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['created'] = "1234567"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.validate_json(invalid))

        # UID is not required, but the format must be correct

        invalid = valid.copy()
        invalid['uid'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.validate_json(invalid))

        # Check that the original one is still valid (i.e. we haven't been getting the validation errors by
        # just damaging the original...

        ScheduleFormat.validate_json(valid)

    def test_from_file(self):
        """
        Check that we can create a ScheduleFormat object from a file-like object,
        and that validation is correctly run.
        """
        valid = {
            "version": 1,
            "tag": "KRONOS-KSF-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
        }

        # Check the validation information

        invalid = valid.copy()
        invalid['tag'] = "wrong-tag"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.from_file(StringIO(json.dumps(invalid))))

        invalid = valid.copy()
        invalid['version'] = "-1"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.from_file(StringIO(json.dumps(invalid))))

        invalid = valid.copy()
        invalid['created'] = "1234567"
        self.assertRaises(jsonschema.ValidationError, lambda: ScheduleFormat.from_file(StringIO(json.dumps(invalid))))

        # Check that the original one is still valid (i.e. we haven't been getting the validation errors by
        # just damaging the original...

        ScheduleFormat.from_file(StringIO(json.dumps(valid)))


if __name__ == "__main__":
    unittest.main()
