#!/usr/bin/env python

import copy
import unittest

# Ensure imports work both in installation, and git, environments
from jsonschema import ValidationError

from kronos_executor.kronos_events import EventComplete, EventTime, EventMetadataChange


class EventTests(unittest.TestCase):

    def test_event_complete(self):

        complete_json_valid_1 = {
            "info":
                {
                    "timestamp": 1.6666,
                    "job": 999,
                    "app": "kronos-synapp"
                 },
            "type": "Complete"
        }

        complete_json_valid_2 = {
            "info":
                {
                    "timestamp": 8888.8888,
                    "job": 999,
                    "app": "kronos-synapp"
                 },
            "type": "Complete"
        }

        complete_json_invalid = {
            "info":
                {
                    "timestamp": 1.527865e+09,
                    "job": 999,
                    "app": "kronos-synapp"
                 },
            "type": "Complete_INVALID_TYPE"
        }

        event_1 = EventComplete(complete_json_valid_1)
        event_2 = EventComplete(complete_json_valid_2)

        # assert get_hash
        self.assertEqual(event_1.get_hashed(), tuple( (("type", "Complete"), ("app", "kronos-synapp"), ("job", 999))) )
        self.assertEqual(event_2.get_hashed(), tuple((("type", "Complete"), ("app", "kronos-synapp"), ("job", 999))))

        # assert event equality
        self.assertEqual(event_1, event_1)

        # assert hashed event equality
        self.assertEqual(event_1.get_hashed(), event_2.get_hashed())

        # test invalid
        self.assertRaises(ValidationError, lambda: EventComplete(complete_json_invalid, validate_event=True))

    def test_event_time(self):

        time_json_valid_1 = {
            "type": "Time",
            "info": {
                "timestamp": 4444444
            }
        }

        time_json_valid_2 = {
            "type": "Time",
            "info": {
                "timestamp": 8888888
            }
        }

        # an invalid one..
        time_json_invalid = copy.deepcopy(time_json_valid_2)
        time_json_invalid["type"] = "Time_INVALID"

        event_1 = EventTime(time_json_valid_1)
        event_2 = EventTime(time_json_valid_2)

        # assert get_hash
        self.assertEqual(event_1.get_hashed(), tuple( ( ("type", "Time"), ("timestamp", 4444444)) ) )
        self.assertEqual(event_2.get_hashed(), tuple( ( ("type", "Time"), ("timestamp", 8888888)) ) )

        # assert event equality
        self.assertNotEqual(event_1, event_2)

        # assert hashed event equality
        self.assertNotEqual(event_1.get_hashed(), event_2.get_hashed())

        # test invalid
        self.assertRaises(ValidationError, lambda: EventTime(time_json_invalid, validate_event=True))

    def test_event_metadata(self):

        meta_json_valid_1 = {
            "type": "MetadataChange",
            "info": {
                "job": 333,
                "app": 3,
            },
            "metadata": {
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4
            }
        }

        meta_json_valid_2 = {
            "type": "MetadataChange",
            "info": {
                "job": 999,
                "app": 9,
            },
            "metadata": {
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4
            }
        }

        # an invalid one..
        meta_json_invalid = copy.deepcopy(meta_json_valid_2)
        meta_json_invalid["type"] = "_INVALID_"

        event_1 = EventMetadataChange(meta_json_valid_1)
        event_2 = EventMetadataChange(meta_json_valid_2)

        # assert get_hash
        self.assertEqual(event_1.get_hashed(),
                                                (
                                                    ("type", "MetadataChange"),
                                                    ("app", 3),
                                                    ("job", 333),
                                                    (
                                                        ("param_1", 1),
                                                        ("param_2", 2),
                                                        ("param_3", 3),
                                                        ("param_4", 4)
                                                    )
                                                )
                         )

        self.assertEqual(event_2.get_hashed(),
                                                (
                                                    ("type", "MetadataChange"),
                                                    ("app", 9),
                                                    ("job", 999),
                                                    (
                                                        ("param_1", 1),
                                                        ("param_2", 2),
                                                        ("param_3", 3),
                                                        ("param_4", 4)
                                                    )
                                                )
                         )

        # assert event equality
        self.assertNotEqual(event_1, event_2)

        # assert hashed event equality
        self.assertNotEqual(event_1.get_hashed(), event_2.get_hashed())

        # test invalid
        self.assertRaises(ValidationError, lambda: EventMetadataChange(meta_json_invalid, validate_event=True))

if __name__ == "__main__":

    unittest.main()
