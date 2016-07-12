
class SyntheticApp(object):

    """ Class representing a synthetic app """

    def __init__(self):

        self.jobID = None
        self.job_name = None
        self.time_start = None
        self.time_signals = None  # time signals
        self.kernels_seq = []

    # load time-series in
    def fill_time_series(self, ts_list):
        self.time_signals = ts_list

    # write list ouf output kernels
    def make_kernels_from_ts(self, n_bins, dgt_types, supported_synth_apps):

        # application object to be written to JSON
        ker_data = {}

        # required metadata
        # "file_read_path": "..."  # (./read_cache)  The directory containing the cached readable files
        # "file_write_path": "..."  # (./write_cache) The directory to write output into
        # "num_procs": 123  # (no check)      The number of MPI processes (for error checking)
        # "start_delay": 230  # (0)             The number of seconds into schedule that a job should be submitted
        # "mpi_ranks_verbose": true  # (false)  Should all MPI processes write to stdout (by default output suppressed)
        # "enable_trace": true  # (false)         Enable trace output of synthetic app execution for debugging
        # "repeat": 123  # (1)             How many times should this job be run (used by the executor)

        # optional metadata
        md_data = {
            "job_ID": self.jobID,
            "job_name": self.job_name,
            "time_start": self.time_start}
        ker_data["metadata"] = md_data

        # ------------- FORMAT --------------
        # {
        #     "mpi_ranks_verbose": true,
        #     "enable_trace": true,
        #
        #     "frames": [{
        #         "name": "file-read",
        #         "kb_read": 12345,
        #         "n_read": 12
        #     }, {
        #         "name": "file-read",
        #         "kb_read": 12345,
        #         "n_read": 12
        #     }, {
        #         "name": "file-write",
        #         "kb_write": 12345,
        #         "n_write": 3
        #     }]
        # }
        # -----------------------------------

        # digitize all the ts
        for (tt, i_ts) in enumerate(self.time_signals):
            i_ts.digitize(n_bins, dgt_types[tt])

        # sort ts by type..
        ker_blocks_all = []
        for i_bin in range(n_bins):
            ker_blocks = {}
            for i_ts in self.time_signals:

                if supported_synth_apps.count(i_ts.ts_group) != 0:
                    # if i_ts.yvalues_bins[i_bin] >= 1.0:
                    if not ker_blocks.has_key(i_ts.ts_group):
                        ker_blocks[i_ts.ts_group] = {i_ts.name: i_ts.yvalues_bins[i_bin], "name": i_ts.ts_group}

                        # TODO: remove this as soon as we count #read and #write..
                        if i_ts.ts_group == "file-read":
                            ker_blocks[i_ts.ts_group]["n_read"] = 1
                            ker_blocks[i_ts.ts_group]["mmap"] = "false"
                        if i_ts.ts_group == "file-write":
                            ker_blocks[i_ts.ts_group]["n_write"] = 1
                            ker_blocks[i_ts.ts_group]["mmap"] = "false"
                        # -----------------------------------------------------------

                    else:
                        ker_blocks[i_ts.ts_group][i_ts.name] = i_ts.yvalues_bins[i_bin]

            # sanitize values in block (e.g. # of mpi calls can't be 0 if kb > 0, etc...)
            ker_blocks['mpi']['n_collective'] = max(1, ker_blocks['mpi']['n_collective'])
            ker_blocks['mpi']['n_pairwise'] = max(1, ker_blocks['mpi']['n_pairwise'])
            ker_blocks['file-write']['kb_write'] = max(1, ker_blocks['file-write']['kb_write'])

            # ker_blocks_all.extend(ker_blocks.values()[:])
            ker_blocks_all.append(ker_blocks.values()[:])

        # return full list of sequences
        ker_data["frames"] = ker_blocks_all

        return ker_data
