import json
import os


class SyntheticApp(object):

    """ Class representing a synthetic app """

    def __init__(self, config_options):

        self.jobID = None
        self.job_name = None
        self.time_start = None
        self.time_signals = None  # time signals
        self.kernels_seq = []
        self.ker_data = None
        self.cpu_freq = config_options.CPU_FREQUENCY

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

        # TODO: this need to be changed to proper value..
        ker_data["num_procs"] = 2

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

                        # Estimated flops from cpu time..
                        if i_ts.name == "cpu_time":
                            sa_name = "flops"
                            FLOPS_GHZ_FACTOR = 4.0
                            ker_blocks[i_ts.ts_group] = {sa_name: i_ts.yvalues_bins[i_bin]*self.cpu_freq/FLOPS_GHZ_FACTOR, "name": i_ts.ts_group}
                            # print "here"
                        else:
                            ker_blocks[i_ts.ts_group] = {i_ts.name: i_ts.yvalues_bins[i_bin], "name": i_ts.ts_group}

                        # TODO: remove this as soon as we count #read and #write..
                        if i_ts.ts_group == "file-read":
                            ker_blocks[i_ts.ts_group]["n_read"] = 1
                            ker_blocks[i_ts.ts_group]["mmap"] = "false"
                        if i_ts.ts_group == "file-write":
                            ker_blocks[i_ts.ts_group]["n_write"] = 10
                            ker_blocks[i_ts.ts_group]["mmap"] = "false"
                        # -----------------------------------------------------------

                    else:
                        ker_blocks[i_ts.ts_group][i_ts.name] = i_ts.yvalues_bins[i_bin]

            # sanitize values in block (e.g. # of mpi calls can't be 0 if kb > 0, etc...)
            # ker_blocks['mpi']['n_collective'] = max(1, int(ker_blocks['mpi']['n_collective']))
            # ker_blocks['mpi']['n_pairwise'] = max(1, int(ker_blocks['mpi']['n_pairwise']))
            # TODO: remove any manual intervention/correction..
            ker_blocks['file-write']['kb_write'] = max(1, ker_blocks['file-write']['kb_write'])
            # ker_blocks['cpu']['flops'] = 1000000000.00

            # ker_blocks_all.extend(ker_blocks.values()[:])
            ker_blocks_all.append(ker_blocks.values()[:])

        # return full list of sequences
        ker_data["frames"] = ker_blocks_all

        return ker_data

    def write_sa_json(self, n_bins, dgt_types, supported_synth_apps, out_dir, idx):
        """ write synthetic apps into json files """

        ker_data = self.make_kernels_from_ts(n_bins, dgt_types, supported_synth_apps)
        self.ker_data = ker_data

        name_json_file = out_dir + '/' + 'job-' + str(idx) + '.json'
        name_json_file_raw = out_dir + '/' + '_temp_input' + str(idx) + '.json'

        with open(name_json_file_raw, 'w') as f:
            json.encoder.FLOAT_REPR = lambda o: format(o, '.2f')
            # json.dump(json_all_synth_app, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))
            json.dump(ker_data, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

        # substitute true string..
        with open(name_json_file, "wt") as fout:
            with open(name_json_file_raw, "rt") as fin:
                for line in fin:
                    # fout.write(line.replace('"true"', 'true'))
                    fout.write(line.replace('"false"', 'false'))

        os.remove(name_json_file_raw)

    def get_json_formatted_data(self):
        """ Return json formatted data """

        return self.ker_data
