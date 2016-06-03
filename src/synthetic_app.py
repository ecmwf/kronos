
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
    def make_kernels_from_ts(self, n_bins):

        # application object to be written to JSON
        ker_data = {}

        # metadata
        md_data = {
            "job_ID": self.jobID,
            "job_name": self.job_name,
            "time_start": self.time_start,
            }

        ker_data["_metadata"] = md_data

        # digitize all the ts
        for i_ts in self.time_signals:
            i_ts.digitize(n_bins, "sum")

        # sort ts by type..
        ker_blocks_all = {}
        cc = 0
        for (bb, i_bin) in enumerate(range(n_bins)):
            ker_blocks = {}
            for i_ts in self.time_signals:

                if not ker_blocks.has_key(i_ts.ts_group):
                    ker_blocks[i_ts.ts_group] = {i_ts.name: i_ts.yvalues[i_bin], "_type": i_ts.ts_group, "_bin": bb}
                    cc += 1
                else:
                    ker_blocks[i_ts.ts_group][i_ts.name] = i_ts.yvalues[i_bin]

            # rename keys to a sequential name..
            for (ii, i_key) in enumerate(ker_blocks.keys()):
                seq_id = "kID-" + str(ii+bb*len(ker_blocks.keys()))
                ker_blocks[seq_id] = ker_blocks.pop(i_key)
                ker_blocks_all[seq_id] = ker_blocks[seq_id]

        # return full list of sequences
        ker_data["kernels"] = ker_blocks_all

        return ker_data
