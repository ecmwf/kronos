#!/usr/bin/env python
import unittest
import tempfile
import os

from config.config import Config
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
                "dir_input": "abcdef",
                "dir_output": "{}",
                "unknown": "{}"
            }}""".format(existing_path, existing_path))
            f.flush()
            self.assertRaises(ConfigurationError, lambda: Config(config_path=f.name))


if __name__ == "__main__":
    unittest.main()
