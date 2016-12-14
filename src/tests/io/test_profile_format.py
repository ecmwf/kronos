#!/usr/bin/env python

from io.profile_format import ProfileFormat

from StringIO import StringIO
import jsonschema
import unittest
import json


class ProfileFormatTest(unittest.TestCase):

    def test_validate(self):
        """
        Construct a dict for a parsed input, and then repeatedly modify it to end up with invalid stuff.
        :return:
        """
        valid = {
            "version": 1,
            "tag": "KRONOS-KPF-MAGIC",
            "created": "2016-12-14T09:57:35Z"  # Timestamp in strict rfc3339 format.
        }

        # Check the validation information

        invalid = valid.copy()
        invalid['tag'] = "wrong-tag"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.from_file(StringIO(json.dumps(invalid))))

        invalid = valid.copy()
        invalid['version'] = "-1"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.from_file(StringIO(json.dumps(invalid))))

        invalid = valid.copy()
        invalid['created'] = "1234567"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.from_file(StringIO(json.dumps(invalid))))

        # Check that the original one is still valid (i.e. we haven't been getting the validation errors by
        # just damaging the original...

        pf = ProfileFormat.from_file(StringIO(json.dumps(valid)))


if __name__ == "__main__":
    unittest.main()
