#!/usr/bin/env python
import unittest
# import tempfile
# import os
import numpy as np

import time_signal
# from config.config import Config
# from exceptions_iows import ConfigurationError
import generator
from jobs import ModelJob
# from model import KronosModel
from workload_data import WorkloadData


class GeneratorTests(unittest.TestCase):

    def test_generator(self):
        """
        The configuration object should have some sane defaults
        """

        # If all of the required arguments are supplied, this should result in a valid job
        ts_complete_set = {tsk: time_signal.TimeSignal.from_values(tsk, [0., 0.1], [1., 999.])
                           for tsk in time_signal.signal_types.keys()}

        valid_args = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': ts_complete_set
        }

        # check that it is a valid job
        job1 = ModelJob(**valid_args)
        job2 = ModelJob(**valid_args)
        job3 = ModelJob(**valid_args)
        job4 = ModelJob(**valid_args)
        job5 = ModelJob(**valid_args)

        input_jobs = [job1, job2, job3, job4, job5]

        # diversify the time start..
        for jj,job in enumerate(input_jobs):
            job.time_start += jj*0.1

        for job in input_jobs:
            self.assertTrue(job.is_valid())

        config_generator = {
                            "type": "match_job_pdf",
                            "random_seed": 0,
                            "tuning_factors": {
                                "kb_collective": 1e-0,
                                "n_collective": 1e-0,
                                "kb_write": 1e-3,
                                "n_pairwise": 10e-0,
                                "n_write": 1e-2,
                                "n_read": 10e-2,
                                "kb_read": 1e-2,
                                "flops": 500e-0,
                                "kb_pairwise": 10e-0
                            },
                            "submit_rate_factor": 8.0,
                            "synthapp_n_cpu": 2,
                            "synthapp_n_nodes": 1,
                            "synthapp_n_frames": 10,
                            "total_submit_interval": 60
                            }

        # create a workload with 5 model jobs
        test_workload = WorkloadData(
                                    jobs=input_jobs,
                                    tag='test_wl'
                                    )

        # create a matrix from jobs timesignals..
        # TODO: this should be done inside the model jobs..
        n_ts_bins = 2
        input_jobs_matrix = np.zeros((len(input_jobs), len(time_signal.signal_types) * n_ts_bins))
        non_value_flag = -999

        # loop over jobs and fill the matrix as appropriate
        for cc, job in enumerate(input_jobs):

            row = []
            for tsk in time_signal.signal_types.keys():
                ts = job.timesignals[tsk]
                if ts is not None:
                    xvals, yvals = ts.digitized(n_ts_bins)
                    row.extend(yvals)
                else:
                    row.extend([non_value_flag for vv in range(0, n_ts_bins)])

            input_jobs_matrix[cc, :] = np.asarray(row)

        clusters = {
                    'source-workload': test_workload.tag,
                    'jobs_for_clustering': test_workload.jobs,
                    'cluster_matrix': input_jobs_matrix[0, :].reshape(1,input_jobs_matrix.shape[1]),
                    'labels': [1 for job in input_jobs],
                    }

        sapps_generator = generator.SyntheticWorkloadGenerator(config_generator, [clusters])
        modelled_sa_jobs = sapps_generator.generate_synthetic_apps()

        # check that the produced sapps are produced from one of the clusters in the matrix..
        self.assertTrue(any(np.equal(input_jobs_matrix, job.timesignals[ts].yvalues).all(1))
                        for job in modelled_sa_jobs
                        for ts in time_signal.signal_types.keys()
                        )

    # def test_dict_override(self):
    #
    #     # existing and not existing paths
    #     existing_path = os.getcwd()
    #     obscure_path = '.__$obscure-nonexistent@'
    #     self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))
    #
    #     # We can override each of the parameters
    #     config_dict = {
    #         'dir_input': existing_path,
    #         'dir_output': existing_path,
    #     }
    #     cfg = Config(config_dict=config_dict)
    #     self.assertEqual(cfg.dir_input, existing_path)
    #     self.assertEqual(cfg.dir_output, existing_path)
    #     # self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])
    #
    #     # Unexpected parameters throw exceptions
    #     config_dict['unexpected'] = 'parameter'
    #     self.assertRaises(ConfigurationError, lambda: Config(config_dict=config_dict))
    #
    # def test_path_override(self):
    #     """
    #     Test the overrides, but put the data into a file
    #     """
    #
    #     # existing and not existing paths
    #     existing_path = os.getcwd()
    #     obscure_path = '.__$obscure-nonexistent@'
    #     self.assertFalse(os.path.exists(os.path.join(os.getcwd(), obscure_path)))
    #
    #     # We should get an exception if we throw idiotic stuff in
    #     self.assertRaises(IOError, lambda: Config(config_path='.__$idiotic-nonexistent@'))
    #
    #     # An empty override dictionary does nothing
    #     with tempfile.NamedTemporaryFile() as f:
    #         f.write("{}")
    #         f.flush()
    #         self.assertRaises(ConfigurationError, lambda: Config(config_dict=f.name))
    #
    #     # We can override each of the parameters
    #     # n.b. Test the comment handling in the  JSON parser
    #     obscure_path = os.path.join(os.getcwd(), '.__$obscure-nonexistent@')
    #     self.assertFalse(os.path.exists(obscure_path))
    #     with tempfile.NamedTemporaryFile() as f:
    #         f.write("""{{
    #             "dir_input": "{}",
    #             #"unknown": "parameter",
    #             "dir_output": "{}"
    #         }}""".format(existing_path, existing_path))
    #         f.flush()
    #         cfg = Config(config_path=f.name)
    #     self.assertEqual(cfg.dir_input, existing_path)
    #     self.assertEqual(cfg.dir_output, existing_path)
    #     # self.assertEqual(cfg.profile_sources, [1, 2, 3, 4])
    #
    #     # Unexpected parameters throw exceptions
    #     with tempfile.NamedTemporaryFile() as f:
    #         f.write("""{{
    #             "dir_input": "abcdef",
    #             "dir_output": "{}",
    #             "unknown": "{}"
    #         }}""".format(existing_path, existing_path))
    #         f.flush()
    #         self.assertRaises(ConfigurationError, lambda: Config(config_path=f.name))


if __name__ == "__main__":
    unittest.main()
