#!/usr/bin/env python

from kronos.io.profile_format import ProfileFormat
# from jobs import ModelJob
# import time_signal

from StringIO import StringIO
from datetime import datetime
import jsonschema
import unittest
import json
import copy


class ProfileFormatTest(unittest.TestCase):

    def test_schema(self):
        """
        Check that the schema method is correctly loading something that looks schema-ish
        """
        s = ProfileFormat.schema()

        self.assertIsInstance(s, dict)
        self.assertEqual(s['$schema'], 'http://json-schema.org/draft-04/schema')
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
            "tag": "KRONOS-KProfile-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
            "workload_tag": "A-tag",
            "profiled_jobs": [{
                "time_start": 537700,
                "time_queued": 99,
                "duration": 147,
                "ncpus": 72,
                "nnodes": 2,
                "time_series": {
                    "kb_read": {
                        "times": [0.01, 0.02, 0.03, 0.04],
                        "values": [15, 16, 17, 18]
                    }
                }
            }]
        }

        # Check the validation information

        invalid = valid.copy()
        invalid['tag'] = "wrong-tag"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['version'] = "-1"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['created'] = "1234567"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['workload_tag'] = 666
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        del invalid['workload_tag']
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0] = "boo hiss"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_start'] = -1
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_start'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_queued'] = -1
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_queued'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['duration'] = -1
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['duration'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['ncpus'] = -1
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['ncpus'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['nnodes'] = -1
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['nnodes'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_series'] = {
            "invalid_ts": {"times": [0.01, 0.02], "values": [10, 20]}
        }
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        del invalid['profiled_jobs'][0]['time_series']['kb_read']['times']
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_series']['kb_read']['times'][3] = 'a'
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        del invalid['profiled_jobs'][0]['time_series']['kb_read']['values']
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        invalid = valid.copy()
        invalid['profiled_jobs'] = copy.deepcopy(valid['profiled_jobs'])
        invalid['profiled_jobs'][0]['time_series']['kb_read']['times'][3] = 'b'
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        # UID is not required, but the format must be correct

        invalid = valid.copy()
        invalid['uid'] = "abcd"
        self.assertRaises(jsonschema.ValidationError, lambda: ProfileFormat.validate_json(invalid))

        # The profiled_jobs list may be empty or omitted

        valid2 = valid.copy()
        del valid2['profiled_jobs']
        ProfileFormat.validate_json(valid2)

        valid2 = valid.copy()
        valid2['profiled_jobs'] = []
        ProfileFormat.validate_json(valid2)

        # Check that the original one is still valid (i.e. we haven't been getting the validation errors by
        # just damaging the original...

        ProfileFormat.validate_json(valid)

        # Check that the data hasn't become inadvertently damaged

        pf = ProfileFormat(json_jobs=valid['profiled_jobs'],
                           created=datetime(2016, 12, 14, 9, 57, 35),
                           uid=valid['uid'])

        self.assertEquals(pf.uid, 1234)
        self.assertEquals(pf.created, datetime(2016, 12, 14, 9, 57, 35))
        self.assertEquals(len(pf.profiled_jobs), 1)
        self.assertEquals(pf.profiled_jobs[0]["time_start"], 537700)
        self.assertEquals(pf.profiled_jobs[0]["time_queued"], 99)
        self.assertEquals(pf.profiled_jobs[0]["duration"], 147)
        self.assertEquals(pf.profiled_jobs[0]["ncpus"], 72)
        self.assertEquals(pf.profiled_jobs[0]["nnodes"], 2)
        self.assertEquals(len(pf.profiled_jobs[0]["time_series"]), 1)
        self.assertEquals(pf.profiled_jobs[0]["time_series"]["kb_read"]["times"][2], 0.03)
        self.assertEquals(pf.profiled_jobs[0]["time_series"]["kb_read"]["values"][2], 17)

    def test_from_file(self):
        """
        Check that we can create a ProfileFormat object from a file-like object,
        and that validation is correctly run.
        """
        valid = {
            "version": 1,
            "tag": "KRONOS-KProfile-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
            "workload_tag": "A-tag",
            "profiled_jobs": [{
                "time_start": 537700,
                "time_queued": 99,
                "duration": 147,
                "ncpus": 72,
                "nnodes": 2,
                "time_series": {
                    "kb_read": {
                        "times": [0.01, 0.02, 0.03, 0.04],
                        "values": [15, 16, 17, 18]
                    }
                }
            }]
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

        # Check that things are read correctly

        self.assertEquals(pf.uid, 1234)
        self.assertEquals(pf.created, datetime(2016, 12, 14, 9, 57, 35))
        self.assertEquals(len(pf.profiled_jobs), 1)
        self.assertEquals(pf.profiled_jobs[0]["time_start"], 537700)
        self.assertEquals(pf.profiled_jobs[0]["time_queued"], 99)
        self.assertEquals(pf.profiled_jobs[0]["duration"], 147)
        self.assertEquals(pf.profiled_jobs[0]["ncpus"], 72)
        self.assertEquals(pf.profiled_jobs[0]["nnodes"], 2)
        self.assertEquals(len(pf.profiled_jobs[0]["time_series"]), 1)
        self.assertEquals(pf.profiled_jobs[0]["time_series"]["kb_read"]["times"][2], 0.03)
        self.assertEquals(pf.profiled_jobs[0]["time_series"]["kb_read"]["values"][2], 17)

    def test_profile_format_equality(self):
        """
        Testing equality
        """
        valid = {
            "version": 1,
            "tag": "KRONOS-KProfile-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
            "workload_tag": "A-tag",
            "profiled_jobs": [{
                "time_start": 537700,
                "time_queued": 99,
                "duration": 147,
                "ncpus": 72,
                "nnodes": 2,
                "time_series": {
                    "kb_read": {
                        "times": [0.01, 0.02, 0.03, 0.04],
                        "values": [15, 16, 17, 18]
                    }
                }
            }]
        }

        pf_valid = ProfileFormat.from_file(StringIO(json.dumps(valid)))

        pf_valid2 = ProfileFormat.from_file(StringIO(json.dumps(valid)))
        self.assertEquals(pf_valid, pf_valid2)

        # Modifying the created/uid attributes doesn't make these differ.

        for name, val in [('created', '2016-12-14T09:57:36Z'),
                          ('uid', 4321)]:

            invalid = copy.deepcopy(valid)
            invalid[name] = val
            pf_invalid = ProfileFormat.from_file(StringIO(json.dumps(invalid)))

            self.assertTrue(pf_valid == pf_invalid)
            self.assertFalse(pf_valid != pf_invalid)

        # Changing _anything_ about the profiled jobs does make the profiles differ

        for name, val in [('ncpus', 73),
                          ('nnodes', 1),
                          ('duration', 99),
                          ('time_queued', 147),
                          ('time_start', 1234),
                          ('time_series', None),
                          ('time_series', {}),
                          ('time_series', {'kb_write': {'times': [0.01], 'values': [15]}}),
                          ('time_series', {'kb_read': {'times': [0.01, 0.02, 0.03, 0.05], "values": [15, 16, 17, 18]}}),
                          ('time_series', {'kb_read': {'times': [0.01, 0.02, 0.03, 0.04], "values": [15, 16, 17, 19]}})
                          ]:

            invalid = copy.deepcopy(valid)
            if val is None:
                del invalid['profiled_jobs'][0][name]
            else:
                invalid['profiled_jobs'][0][name] = val
            pf_invalid = ProfileFormat.from_file(StringIO(json.dumps(invalid)))

            self.assertTrue(pf_valid != pf_invalid)
            self.assertFalse(pf_valid == pf_invalid)

    def test_default_workload_tag(self):

        pf = ProfileFormat(model_jobs=[])
        self.assertEquals(pf.workload_tag, "unknown")

        pf = ProfileFormat(model_jobs=[], workload_tag="my-tag")
        self.assertEquals(pf.workload_tag, "my-tag")


if __name__ == "__main__":
    unittest.main()
