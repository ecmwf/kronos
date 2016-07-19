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
        cfg = Config()

        self.assertEqual(cfg.dir_input, os.path.join(os.getcwd(), 'input'))
        self.assertEqual(cfg.dir_output, os.path.join(os.getcwd(), 'output'))

        self.assertEqual(cfg.profile_sources, [])

    def test_dict_override(self):

        # An empty ovirride dictionary does nothing
        cfg = Config(config_dict={})
        self.assertEqual(cfg.dir_input, os.path.join(os.getcwd(), 'input'))
        self.assertEqual(cfg.dir_output, os.path.join(os.getcwd(), 'output'))
        self.assertEqual(cfg.profile_sources, [])

        # We can override each of the parameters
        obscure_path = '.__$obscure-nonexistent@'
        self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))
        config_dict = {
            'dir_input': 'abcdef',
            'dir_output': obscure_path,
            'profile_sources': [1, 2, 3, 4]
        }
        cfg = Config(config_dict=config_dict)
        self.assertEqual(cfg.dir_input, 'abcdef')
        self.assertEqual(cfg.dir_output, obscure_path)
        self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])
        self.assertTrue(os.path.exists(os.path.join(os.getcwd(), obscure_path)))
        os.rmdir(os.path.join(os.getcwd(), obscure_path))

        # Unexpected parameters throw exceptions
        config_dict['unexpected'] = 'parameter'
        self.assertRaises(ConfigurationError, lambda: Config(config_dict=config_dict))

    def test_path_override(self):
        """
        Test the overrides, but put the data into a file
        """
        # We should get an exception if we throw idiotic stuff in
        self.assertRaises(IOError, lambda: Config(config_path='.__$idiotic-nonexistent@'))

        # An empty override dictionary does nothing
        with tempfile.NamedTemporaryFile() as f:
            f.write("{}")
            f.flush()
            cfg = Config(config_path=f.name)
        self.assertEqual(cfg.dir_input, os.path.join(os.getcwd(), 'input'))
        self.assertEqual(cfg.dir_output, os.path.join(os.getcwd(), 'output'))
        self.assertEqual(cfg.profile_sources, [])

        # We can override each of the parameters
        # n.b. Test the comment handling in the  JSON parser
        obscure_path = os.path.join(os.getcwd(), '.__$obscure-nonexistent@')
        self.assertFalse(os.path.exists(obscure_path))
        with tempfile.NamedTemporaryFile() as f:
            f.write("""{{
                "dir_input": "abcdef",
                "dir_output": "{}",
                #"unknown": "parameter",
                "profile_sources": [1, 2, 3, 4]
            }}""".format(obscure_path))
            f.flush()
            cfg = Config(config_path=f.name)
        self.assertEqual(cfg.dir_input, 'abcdef')
        self.assertEqual(cfg.dir_output, obscure_path)
        self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])
        self.assertTrue(os.path.exists(obscure_path))
        os.rmdir(obscure_path)

        # Unexpected parameters throw exceptions
        with tempfile.NamedTemporaryFile() as f:
            f.write("""{{
                "dir_input": "abcdef",
                "dir_output": "{}",
                "unknown": "parameter",
                "profile_sources": [1, 2, 3, 4]
            }}""".format(obscure_path))
            f.flush()
            self.assertRaises(ConfigurationError, lambda: Config(config_path=f.name))





if __name__ == "__main__":
    unittest.main()
