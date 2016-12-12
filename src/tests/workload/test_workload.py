#!/usr/bin/env python
import unittest
import numpy as np

import time_signal
from jobs import ModelJob
from workload_data import WorkloadData


class WorkloadTests(unittest.TestCase):

    def test_workload_data(self):

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
        for jj, job in enumerate(input_jobs):
            job.time_start += jj * 0.1

        for job in input_jobs:
            self.assertTrue(job.is_valid())

        # create a workload with 5 model jobs
        test_workload = WorkloadData(
                                    jobs=input_jobs,
                                    tag='test_wl'
                                    )

        # ------- verify that all the jobs in workload are actually the initial jobs provided --------
        self.assertTrue(all(job is input_jobs[jj] for jj, job in enumerate(test_workload.jobs)))

        # ------------ verify sums of timesignals -------------------
        for ts_name in time_signal.signal_types:
            ts_sum = 0
            for j in input_jobs:
                ts_sum += sum(j.timesignals[ts_name].yvalues)

            # verify the sums..
            self.assertEqual(ts_sum, test_workload.total_metrics_sum_dict[ts_name])

        # ------------ verify global time signals -------------------
        valid_args_1 = {
            'time_start': 0.1,
            'duration': 0.222,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                           for tsk in time_signal.signal_types.keys()}
        }
        job1 = ModelJob(**valid_args_1)

        valid_args_2 = {
            'time_start': 0.1,
            'duration': 0.333,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                           for tsk in time_signal.signal_types.keys()}
        }
        job2 = ModelJob(**valid_args_2)

        test_workload = WorkloadData(jobs=[job1, job2], tag='wl_2jobs')

        for job in [job1, job2]:
            for ts in time_signal.signal_types:
                self.assertTrue(all(v+job.time_start in test_workload.total_metrics_timesignals[ts].xvalues for v in job.timesignals[ts].xvalues))
                self.assertTrue(all(v in test_workload.total_metrics_timesignals[ts].yvalues for v in job.timesignals[ts].yvalues))
        # -----------------------------------------------------------

    def test_workload_fillin_default(self):

        # create 2 random jobs..
        valid_args_1 = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in time_signal.signal_types.keys()}
        }
        job1 = ModelJob(**valid_args_1)

        valid_args_2 = {
            'time_start': 0.1,
            'duration': 0.2,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in time_signal.signal_types.keys()}
        }
        job2 = ModelJob(**valid_args_2)

        test_workload = WorkloadData(jobs=[job1, job2], tag='wl_2jobs')

        # ---------------------- fill in config -----------------------
        fillin_funct_config = [
                                {
                                    "type": "step",
                                    "name": "step-1",
                                    "x_step": 0.5
                                },
                                {
                                    "type": "custom",
                                    "name": "custom-1",
                                    "x_values": [0, 0.1, 0.15, 0.3333, 0.5, 0.8, 0.9, 1.0],
                                    "y_values": [0, 0.1, 0.2, 0.3, 0.5, 0.8, 0.9, 1.0]
                                }
                                ]

        default_config = {
                            "type": "fill_missing_entries",
                            "apply_to": ["operational-ipm"],
                            "priority": 0,
                            "metrics": {
                                        "kb_collective": [100, 1000],
                                        "n_collective": [10, 1000],
                                        "kb_write": {"function": "step-1",
                                                     "scaling": 1000.0
                                                     },
                                        "n_pairwise": {"function": "custom-1",
                                                       "scaling": 1000.0
                                                       },
                                        "n_write": [10, 1000],
                                        "n_read": [10, 1000],
                                        "kb_read": [100, 1000],
                                        "flops": [1e6, 1e8],
                                        "kb_pairwise": [100, 1000]
                                        }
                        }

        test_workload.apply_default_metrics(default_config, fillin_funct_config)

        # test that the random number generated is within the range considered
        for j in test_workload.jobs:

            self.assertTrue(all(min(default_config['metrics']['kb_collective']) <= v <= max(default_config['metrics']['kb_collective'])
                                for v in j.timesignals['kb_collective'].yvalues))

            self.assertTrue(all(min(default_config['metrics']['n_collective']) <= v <= max(default_config['metrics']['n_collective'])
                                for v in j.timesignals['n_collective'].yvalues))

            self.assertTrue(all(min(default_config['metrics']['n_write']) <= v <= max(default_config['metrics']['n_write'])
                                for v in j.timesignals['n_write'].yvalues))

            self.assertTrue(all(min(default_config['metrics']['n_read']) <= v <= max(default_config['metrics']['n_read'])
                                for v in j.timesignals['n_read'].yvalues))

            self.assertTrue(all(min(default_config['metrics']['kb_read']) <= v <= max(default_config['metrics']['kb_read'])
                                for v in j.timesignals['kb_read'].yvalues))

            self.assertTrue(all(min(default_config['metrics']['flops']) <= v <= max(default_config['metrics']['flops'])
                                for v in j.timesignals['flops'].yvalues))

            self.assertTrue(all(min(default_config['metrics']['kb_pairwise']) <= v <= max(default_config['metrics']['kb_pairwise'])
                                for v in j.timesignals['kb_pairwise'].yvalues))

        # test the values assigned by the custom function:
        for j in test_workload.jobs:
            self.assertTrue(all(v/float(default_config['metrics']['n_pairwise']['scaling']) in fillin_funct_config[1]['y_values'] for v in j.timesignals['n_pairwise'].yvalues))

    def test_workload_fillin_match(self):

        # ------------ verify global time signals -------------------
        valid_args_1 = {
            'job_name': "job1",
            'time_start': 0.1,
            'duration': 0.222,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.random.rand(10), np.arange(10)*2)
                            for tsk in time_signal.signal_types.keys()}
        }
        job1 = ModelJob(**valid_args_1)

        valid_args_2 = {
            'job_name': "job_match",
            'time_start': 0.1,
            'duration': 0.333,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.random.rand(10), np.random.rand(10))
                            for tsk in time_signal.signal_types.keys()}
        }
        job2 = ModelJob(**valid_args_2)

        valid_args_3 = {
            'job_name': "job_match",
            'time_start': 0.1,
            'duration': 0.333,
            'ncpus': 1,
            'nnodes': 1,
            'timesignals': {tsk: time_signal.TimeSignal.from_values(tsk, np.arange(10), np.arange(10))
                            for tsk in time_signal.signal_types.keys()}
        }

        job3 = ModelJob(**valid_args_3)

        test_wl = WorkloadData(jobs=[job1, job2], tag='wl_test')
        source_wl = WorkloadData(jobs=[job3], tag='wl_match_source')

        test_wl.apply_lookup_table(source_wl, 1.0, 10, ['job_name'])

        # not match with job 1
        self.assertFalse(all(np.array_equal(test_wl.jobs[0].timesignals[ts].yvalues,
                                           source_wl.jobs[0].timesignals[ts].yvalues)
                            for ts in time_signal.signal_types.keys()))

        # match with job 2
        self.assertTrue(all(np.array_equal(test_wl.jobs[1].timesignals[ts].yvalues,
                                           source_wl.jobs[0].timesignals[ts].yvalues)
                            for ts in time_signal.signal_types.keys()))

    def test_workload_fillin_RS(self):
        # TODO: to write..
        pass


if __name__ == "__main__":
    unittest.main()
