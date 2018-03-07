# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import json
import unittest
from StringIO import StringIO

import jsonschema
from kronos.io.format_data_handlers.kschedule_data import KScheduleData


class ProfileFormatTest(unittest.TestCase):

    valid_kschedule = {
        "unscaled_metrics_sums": {
            "kb_collective": 208517553.62158203,
            "n_collective": 4027725.0,
            "kb_write": 741712101.625,
            "n_pairwise": 21589693.0,
            "n_write": 32810.097499999974,
            "n_read": 554277.9349999997,
            "kb_read": 728008033.3916016,
            "flops": 95136577882416.19,
            "kb_pairwise": 673515252.5620931
        },
        "jobs": [
            {
                "frames": [
                    [
                        {
                            "flops": 333,
                            "name": "cpu"
                        },
                        {
                            "n_write": 10,
                            "n_files": 1,
                            "name": "file-write",
                            "kb_write": 2500.0
                        },
                        {
                            "n_write": 10,
                            "n_files": 1,
                            "name": "file-write",
                            "kb_write": 2500.0
                        }
                    ],
                    [
                        {
                            "kb_read": 999,
                            "invalidate": True,
                            "mmap": True,
                            "name": "file-read",
                            "n_read": 111
                        }

                    ]
                ],
                "depends": [],
                "num_procs": 1,
                "metadata": {
                    "job_name": "dummy-appID-0",
                    "workload_name": "dummy-workload"
                },
                "start_delay": 0
            },
            {
                "frames": [
                    [
                        {
                            "flops": 1000,
                            "name": "cpu"
                        },
                        {
                            "n_write": 10,
                            "n_files": 1,
                            "name": "file-write",
                            "kb_write": 2500.0
                        },
                        {
                            "kb_read": 500,
                            "invalidate": True,
                            "mmap": True,
                            "name": "file-read",
                            "n_read": 20
                        },
                        {
                            "n_collective": 444,
                            "kb_collective": 888,
                            "kb_pairwise": 222,
                            "name": "mpi",
                            "n_pairwise": 444
                        },
                        {
                            "n_collective": 444,
                            "kb_collective": 888,
                            "kb_pairwise": 222,
                            "name": "mpi",
                            "n_pairwise": 444
                        }
                    ]
                ],
                "depends": [0],
                "num_procs": 5,
                "metadata": {
                    "job_name": "dummy-appID-1",
                    "workload_name": "dummy-workload"
                },
                "start_delay": 0
            }
        ],
        "uid": 4426,
        "created": "2017-06-16T15:04:06Z",
        "tag": "KRONOS-KSCHEDULE-MAGIC",
        "version": 3,
        "scaling_factors": {
            "flops": 1.0,
            "n_collective": 1.0,
            "kb_write": 1.0,
            "n_pairwise": 1.0,
            "n_write": 1.0,
            "n_read": 1.0,
            "kb_read": 1.0,
            "kb_collective": 1.0,
            "kb_pairwise": 1.0
        }
    }

    def test_validate(self):

        # Check the validation information
        valid_ks = self.valid_kschedule.copy()
        self.assertRaises(jsonschema.ValidationError, lambda: KScheduleData.validate_json(StringIO(json.dumps(valid_ks))))

    def test_flops_series(self):

        # Check the flops series (per job)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        flops_job = KScheduleData.per_job_series(jobs, "flops")
        self.assertEqual(flops_job, [333, 1000])

        # Check the flops series (per kernel)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        flops_ker = KScheduleData.per_kernel_series(jobs, "flops")
        self.assertEqual(flops_ker, [333, 1000])

        # Check the flops series (per process)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        flops_proc = KScheduleData.per_process_series(jobs, "flops")
        self.assertEqual(flops_proc, [333, 200, 200, 200, 200, 200])

        # Check the flops series (per call)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        self.assertRaises(RuntimeError, lambda: KScheduleData.per_call_series(jobs, "flops"))

    def test_io_write_series(self):

        # Check the io series (per job)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_write_job = KScheduleData.per_job_series(jobs, "kb_write")
        self.assertEqual(io_write_job, [5000, 2500])

        # Check the io series (per kernel)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_write_kernel = KScheduleData.per_kernel_series(jobs, "kb_write")
        self.assertEqual(io_write_kernel, [2500, 2500, 2500])

        # Check the io series (per process)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_write_process = KScheduleData.per_process_series(jobs, "kb_write")
        self.assertEqual(io_write_process, [2500, 2500, 500, 500, 500, 500, 500])

        # Check the flops series (per call)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_write_call = KScheduleData.per_call_series(jobs, "kb_write")
        self.assertEqual(io_write_call, [250]*20 + [250]*10)

    def test_io_read_series(self):

        # Check the io series (per job)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_read_job = KScheduleData.per_job_series(jobs, "kb_read")
        self.assertEqual(io_read_job, [999, 500])

        # Check the io series (per kernel)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_read_kernel = KScheduleData.per_kernel_series(jobs, "kb_read")
        self.assertEqual(io_read_kernel, [999, 500])

        # Check the io series (per process)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_read_process = KScheduleData.per_process_series(jobs, "kb_read")
        self.assertEqual(io_read_process, [999]+[100]*5)

        # Check the io series (per call)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        io_read_call = KScheduleData.per_call_series(jobs, "kb_read")
        self.assertEqual(io_read_call, [9]*111+[25]*20)

    def test_mpi_pairwise(self):

        # Check the mpi p2p series (per job)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_job = KScheduleData.per_job_series(jobs, "kb_pairwise")
        self.assertEqual(mpi_job, [0, 222*2])

        # Check the mpi p2p series (per kernel)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_kernel = KScheduleData.per_kernel_series(jobs, "kb_pairwise")
        self.assertEqual(mpi_kernel, [222, 222])

        # Check the mpi p2p series (per process)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_process = KScheduleData.per_process_series(jobs, "kb_pairwise")
        self.assertEqual(mpi_process, [222]*5+[222]*5)

        # Check the mpi p2p series (per call)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_call = KScheduleData.per_call_series(jobs, "kb_pairwise")
        self.assertEqual(mpi_call, [222/444.0]*444+[222/444.0]*444)

    def test_mpi_collective(self):

        # Check the mpi p2p series (per job)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_job = KScheduleData.per_job_series(jobs, "kb_collective")
        self.assertEqual(mpi_job, [0, 888*2])

        # Check the mpi p2p series (per kernel)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_kernel = KScheduleData.per_kernel_series(jobs, "kb_collective")
        self.assertEqual(mpi_kernel, [888, 888])

        # Check the mpi p2p series (per process)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_process = KScheduleData.per_process_series(jobs, "kb_collective")
        self.assertEqual(mpi_process, [888]*5+[888]*5)

        # Check the mpi p2p series (per call)
        valid_ks = self.valid_kschedule.copy()
        jobs = KScheduleData.from_file(StringIO(json.dumps(valid_ks))).jobs
        mpi_call = KScheduleData.per_call_series(jobs, "kb_collective")
        self.assertEqual(mpi_call, [888/444.0]*444+[888/444.0]*444)


if __name__ == "__main__":
    unittest.main()
