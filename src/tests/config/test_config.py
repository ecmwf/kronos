#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import json
import unittest
import tempfile
import os
from StringIO import StringIO

import jsonschema

from config.config import Config
from config.config_format import ConfigFormat
from exceptions_iows import ConfigurationError


class ConfigTests(unittest.TestCase):

    def test_defaults(self):
        """
        The configuration object should have some sane defaults
        """

        # existing and not existing paths
        existing_path = os.getcwd()
        obscure_path = '.__$obscure-nonexistent@'
        self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))

        # this should not throw a ConfigurationError as input dir is not set
        self.assertRaises(ConfigurationError, lambda: Config(config_dict={'dir_output': existing_path}))

        # this should not throw a ConfigurationError as output dir is not set
        self.assertRaises(ConfigurationError, lambda: Config(config_dict={'dir_input': existing_path}))

        # this should not throw a ConfigurationError as input dir is not existent
        self.assertRaises(ConfigurationError, lambda: Config(config_dict={'dir_input': obscure_path,
                                                                          'dir_output': existing_path}))

        # this should not throw a ConfigurationError as output dir is not existent
        self.assertRaises(ConfigurationError, lambda: Config(config_dict={'dir_input': obscure_path,
                                                                          'dir_output': existing_path}))

    def test_dict_override(self):

        # existing and not existing paths
        existing_path = os.getcwd()
        obscure_path = '.__$obscure-nonexistent@'
        self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))

        # We can override each of the parameters
        config_dict = {
            'dir_input': existing_path,
            'dir_output': existing_path,
            'model': {'non-empty_dict': 'dummy_val'}
        }
        cfg = Config(config_dict=config_dict)
        self.assertEqual(cfg.dir_input, existing_path)
        self.assertEqual(cfg.dir_output, existing_path)
        # self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])

        # Unexpected parameters throw exceptions
        config_dict['unexpected'] = 'parameter'
        self.assertRaises(ConfigurationError, lambda: Config(config_dict=config_dict))

    def test_path_override(self):
        """
        Test the overrides, but put the data into a file
        """

        # existing and not existing paths
        existing_path = os.getcwd()
        obscure_path = '.__$obscure-nonexistent@'
        self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))

        # We should get an exception if we throw idiotic stuff in
        self.assertRaises(IOError, lambda: Config(config_path='.__$idiotic-nonexistent@'))

        # An empty override dictionary does nothing
        with tempfile.NamedTemporaryFile() as f:
            f.write("{}")
            f.flush()
            self.assertRaises(ConfigurationError, lambda: Config(config_dict=f.name))

        # We can override each of the parameters
        # n.b. Test the comment handling in the  JSON parser
        obscure_path = os.path.join(os.getcwd(), '.__$obscure-nonexistent@')
        self.assertFalse(os.path.exists(obscure_path))
        with tempfile.NamedTemporaryFile() as f:
            f.write("""{{
                "dir_input": "{}",
                #"unknown": "parameter",
                "dir_output": "{}",
                "model": {{\"non-empty_dict\": \"dummy_val\"}}
            }}""".format(existing_path, existing_path))
            f.flush()
            cfg = Config(config_path=f.name)
        self.assertEqual(cfg.dir_input, existing_path)
        self.assertEqual(cfg.dir_output, existing_path)
        # self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])

        # Unexpected parameters throw exceptions
        with tempfile.NamedTemporaryFile() as f:
            f.write("""{{
                "dir_input": "abcdef",
                "dir_output": "{}",
                "unknown": "{}"
            }}""".format(existing_path, existing_path))
            f.flush()
            self.assertRaises(ConfigurationError, lambda: Config(config_path=f.name))

    def test_config_model_structure_from_file(self):

        valid = {
            "verbose": True,
            "version": 1,
            "tag": "KRONOS-CONFIG-MAGIC",
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "uid": 1234,
            "dir_input": "input",  # Timestamp in strict rfc3339 format.
            "dir_output": "output",
            "kpf_files": ["file1", "file2"],
            "ksf_filename": "ksf_output",
            "model": {
                "fill_in": {},
                "classification": {},
                "generator": {}
            }
        }

        # check that no exceptions are raised
        self.assert_(jsonschema.ValidationError, lambda: ConfigFormat.from_file(StringIO(json.dumps(valid))))


if __name__ == "__main__":
    unittest.main()
