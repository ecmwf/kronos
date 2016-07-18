import time_signal


class SyntheticApp(object):

    """ Class representing a synthetic app """

    def __init__(self, job_name=None, time_signals=None):

        self.jobID = None
        self.job_name = job_name
        self.time_start = None
        self.time_signals = time_signals
        self.kernels_seq = []

    # write list ouf output kernels
    def make_kernels_from_ts(self, n_bins, supported_synth_apps):

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
        for ts_name, ts in self.time_signals.iteritems():
            ts.digitize(n_bins, time_signal.signal_types[ts_name]['behaviour'])

        # sort ts by type..
        ker_blocks_all = []
        for i_bin in range(n_bins):
            ker_blocks = {}
            for ts_name, ts in self.time_signals.iteritems():

                if supported_synth_apps.count(ts.ts_group) != 0:
                    # if i_ts.yvalues_bins[i_bin] >= 1.0:
                    if not ker_blocks.has_key(ts.ts_group):
                        ker_blocks[ts.ts_group] = {ts.name: ts.yvalues_bins[i_bin], "name": ts.ts_group}

                        # TODO: remove this as soon as we count #read and #write..
                        if ts.ts_group == "file-read":
                            ker_blocks[ts.ts_group]["n_read"] = 1
                            ker_blocks[ts.ts_group]["mmap"] = False
                        elif ts.ts_group == "file-write":
                            ker_blocks[ts.ts_group]["n_write"] = 1
                            ker_blocks[ts.ts_group]["mmap"] = False
                        # -----------------------------------------------------------

                    else:
                        ker_blocks[ts.ts_group][ts.name] = ts.yvalues_bins[i_bin]

            # sanitize values in block (e.g. # of mpi calls can't be 0 if kb > 0, etc...)
            ker_blocks['mpi']['n_collective'] = max(1, ker_blocks['mpi']['n_collective'])
            ker_blocks['mpi']['n_pairwise'] = max(1, ker_blocks['mpi']['n_pairwise'])
            ker_blocks['file-write']['kb_write'] = max(1, ker_blocks['file-write']['kb_write'])

            # ker_blocks_all.extend(ker_blocks.values()[:])
            ker_blocks_all.append(ker_blocks.values()[:])

        # return full list of sequences
        ker_data["frames"] = ker_blocks_all

        return ker_data
