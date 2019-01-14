#!/usr/bin/env python
# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import copy
import json
import os
import tempfile
import unittest
from StringIO import StringIO

import jsonschema
from kronos_modeller.config.config import Config
from kronos_modeller.config.config_format import ConfigFormat

from kronos_modeller.exceptions_iows import ConfigurationError


class ConfigTests(unittest.TestCase):
    def test_defaults(self):
        """
        The configuration object should have some sane defaults
        """

        # existing and not existing paths
        existing_path = os.getcwd()
        obscure_path = '.__$obscure-nonexistent@'
        self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))

        config_dict = {
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            'dir_input': existing_path,
            'dir_output': existing_path,
            "kprofile_files": ["file1", "file2"],
            "kschedule_filename": "kschedule_output"
        }

        # this should not throw a ConfigurationError as input dir is not existent
        config_dict_non_val = config_dict
        config_dict_non_val['dir_input'] = obscure_path
        config_dict_non_val['dir_output'] = existing_path
        self.assertRaises(ConfigurationError, lambda: Config(config_dict=config_dict_non_val))

        # this should not throw a ConfigurationError as output dir is not existent
        config_dict_non_val = config_dict
        config_dict_non_val['dir_input'] = existing_path
        config_dict_non_val['dir_output'] = obscure_path
        self.assertRaises(ConfigurationError, lambda: Config(config_dict=config_dict_non_val))

    def test_dict_override(self):
        # existing and not existing paths
        existing_path = os.getcwd()
        obscure_path = '.__$obscure-nonexistent@'
        self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))

        # We can override each of the parameters
        config_dict = {
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            'dir_input': existing_path,
            'dir_output': existing_path,
            "kprofile_files": ["file1", "file2"],
            "kschedule_filename": "kschedule_output"
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
                "created": "2016-12-14T09:57:35Z",
                "kprofile_files": ["file1", "file2"],
                "kschedule_filename": "kschedule_output",
                #"unknown": "parameter",
                "dir_output": "{}"
            }}""".format(existing_path, existing_path))
            f.flush()
            cfg = Config(config_path=f.name)
        self.assertEqual(cfg.dir_input, existing_path)
        self.assertEqual(cfg.dir_output, existing_path)
        # self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])

        # Unexpected parameters throw exceptions
        with tempfile.NamedTemporaryFile() as f:
            f.write("""{{
                "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
                "dir_input": "abcdef",
                "dir_output": "{}",
                "kprofile_files": ["file1", "file2"],
                "kschedule_filename": "kschedule_output",
                "unknown": "{}"
            }}""".format(existing_path, existing_path))
            f.flush()
            self.assertRaises(ConfigurationError, lambda: Config(config_path=f.name))

    def test_config_model_structure_from_file(self):
        # ------------- generic/valid config file ---------------
        config_empty_model = {
            "verbose": True,
            "created": "2016-12-14T09:57:35Z",  # Timestamp in strict rfc3339 format.
            "dir_input": "input",  # Timestamp in strict rfc3339 format.
            "dir_output": "output",
            "kprofile_files": ["file1", "file2"],
            "kschedule_filename": "kschedule_output"
        }

        # check that no exceptions are raised - empty "model" is OK
        ConfigFormat.from_file(StringIO(json.dumps(config_empty_model)))

        # ------------- invalid "model" config file (INV-0) ----------
        config_invalid_0 = config_empty_model
        config_invalid_0["model"] = {}

        # ------------- invalid "model" config file (INV-1) ----------
        config_invalid_1 = config_empty_model
        config_invalid_1["model"] = {
            "fill_in": {},
            "classification": {},
            "generator": {}
        }

        # check that exception is raised
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_1))))

        # ------------- invalid "model" config file (INV-2) ----------
        # NB: classification and generator are required and need required fields..
        # so they cannot be empty
        config_invalid_2 = config_empty_model
        config_invalid_2["model"] = {
            "classification": {},
            "generator": {}
        }

        # check that exception is raised
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_2))))
        # --------------------------------------------------------

        # ------------- invalid "model" config file (INV-3) ----------
        # NB: classification.operations cannot be empty - either is there or not there
        config_invalid_3 = copy.deepcopy(config_empty_model)
        config_invalid_3["model"] = {
            "classification": {
                "operations": [],
                "clustering": {
                    "apply_to": [
                        "HR_other",
                        "ENS_data_assimilation",
                        "ENS_other",
                        "law",
                        "Other",
                        "HR_model",
                        "ENS_model"
                    ],
                    "type": "Kmeans",
                    "ok_if_low_rank": True,
                    "user_does_not_check": True,
                    "rseed": 0,
                    "max_iter": 100,
                    "max_num_clusters": 20,
                    "delta_num_clusters": 1,
                    "num_timesignal_bins": 5
                }
            },
            "generator": {
                "type": "match_job_pdf_exact",
                "n_bins_for_pdf": 20,
                "random_seed": 0,
                "scaling_factors": {
                    "kb_collective": 1e1,
                    "n_collective": 1e-1,
                    "kb_write": 1e-4,
                    "n_pairwise": 1e-1,
                    "n_write": 1e-4,
                    "n_read": 1e-3,
                    "kb_read": 1e-3,
                    "flops": 1e1,
                    "kb_pairwise": 10e-0
                },
                "submit_rate_factor": 4.0,
                "synthapp_n_cpu": 2,
                "total_submit_interval": 300
            }
        }
        # check that exception is raised
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_3))))

        # ------------- invalid "model" config file (INV-4) ----------
        # invalid clustering entries
        config_invalid_4 = copy.deepcopy(config_empty_model)
        config_invalid_4["model"] = {
            "classification": {
                "clustering": {
                    "aasdasdfgsdfgdfg1": "adfgsdfgd",
                    "aasdasdfgsdfgdfg2": "adfgsdfgd",
                    "aasdasdfgsdfgdfg3": "adfgsdfgd",
                    "aasdasdfgsdfgdfg4": "adfgsdfgd",
                }
            },
            "generator": {
                "type": "match_job_pdf_exact",
                "n_bins_for_pdf": 20,
                "random_seed": 0,
                "scaling_factors": {
                    "kb_collective": 1e1,
                    "n_collective": 1e-1,
                    "kb_write": 1e-4,
                    "n_pairwise": 1e-1,
                    "n_write": 1e-4,
                    "n_read": 1e-3,
                    "kb_read": 1e-3,
                    "flops": 1e1,
                    "kb_pairwise": 10e-0
                },
                "submit_rate_factor": 4.0,
                "synthapp_n_cpu": 2,
                "total_submit_interval": 300
            }
        }
        # check that exception is raised
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_4))))

        # ------------- invalid "model" config file (INV-5) ----------
        # NB: missing clustering keys
        config_invalid_5 = copy.deepcopy(config_invalid_3)
        config_invalid_5["model"]["classification"].pop("operations") # empty operations

        config_invalid_5["model"]["classification"]["clustering"] = {
                                                                    "apply_to": [
                                                                        "HR_other",
                                                                        "ENS_data_assimilation",
                                                                        "ENS_other"
                                                                    ],
                                                                    "type": "Kmeans",
                                                                    # "ok_if_low_rank": True,
                                                                    # "user_does_not_check": True,
                                                                    # "rseed": 0,
                                                                    "max_iter": 100,
                                                                    "max_num_clusters": 20,
                                                                    "delta_num_clusters": 1,
                                                                    "num_timesignal_bins": 5
                                                                    }

        # check that exception is raised
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_5))))

        # ------------- invalid "model" config file (INV-6) ----------
        # NB: missing clustering keys
        config_invalid_6 = copy.deepcopy(config_invalid_3)
        config_invalid_6["model"]["classification"].pop("operations")  # empty operations

        config_invalid_6["model"]["classification"]["clustering"] = {
            "apply_to": [
                "HR_other",
                "ENS_data_assimilation",
                "ENS_other"
            ],
            "type": "Kmeans",
            # "ok_if_low_rank": True,
            # "user_does_not_check": True,
            # "rseed": 0,
            "max_iter": 100,
            "max_num_clusters": 20,
            "delta_num_clusters": 1,
            "num_timesignal_bins": 5
        }

        # check that exception is raised
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_6))))
        # ==========================================================================================

        # ------------- invalid "model" config file (INV-7) ----------
        # NB: mis-spelt operation key
        config_invalid_7 = copy.deepcopy(config_invalid_3)
        config_invalid_7["model"]["classification"]["operations"].append(
            {
                "type": "split_wrong_keys",
                "apply_to": "operational-ipm",
                "create_workload": "law",
                "split_by": "label",
                "keywords_in": ["law/"],
                "keywords_out": []
            })

        # check that exception is raised
        # pprint(config_invalid_7)
        # ConfigFormat.from_file(StringIO(json.dumps(config_invalid_7)))
        self.assertRaises(jsonschema.ValidationError,
                          lambda: ConfigFormat.from_file(StringIO(json.dumps(config_invalid_7))))
        # ==========================================================================================

        # ==========================================================================================
        # ------------- valid "model" config file (VAL-0) ----------
        # NB: classification.operations can be empty
        config_valid_0 = config_empty_model
        config_valid_0["model"] = {
            "classification": {

                "clustering": {
                    "apply_to": [
                        "HR_other",
                        "ENS_data_assimilation",
                        "ENS_other",
                        "law",
                        "Other",
                        "HR_model",
                        "ENS_model"
                    ],
                    "type": "Kmeans",
                    "ok_if_low_rank": True,
                    "user_does_not_check": True,
                    "rseed": 0,
                    "max_iter": 100,
                    "max_num_clusters": 20,
                    "delta_num_clusters": 1,
                    "num_timesignal_bins": 5
                }
            },
            "generator": {
                "type": "match_job_pdf_exact",
                "n_bins_for_pdf": 20,
                "random_seed": 0,
                "scaling_factors": {
                    "kb_collective": 1e1,
                    "n_collective": 1e-1,
                    "kb_write": 1e-4,
                    "n_pairwise": 1e-1,
                    "n_write": 1e-4,
                    "n_read": 1e-3,
                    "kb_read": 1e-3,
                    "flops": 1e1,
                    "kb_pairwise": 10e-0
                },
                "submit_rate_factor": 4.0,
                "synthapp_n_cpu": 2,
                "total_submit_interval": 300
            }
        }

        # check that no exceptions are raised - empty "model" is OK
        ConfigFormat.from_file(StringIO(json.dumps(config_valid_0)))


if __name__ == "__main__":
    unittest.main()
