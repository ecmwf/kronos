import json
from kronos_tools.print_colour import print_colour
import time_signal


class KSFFileHandler(object):
    """
    Class that encapsulates some of the operations for a ksf file
    """
    def __init__(self):

        self.synth_app_data = None
        self.synth_app_json_data = None
        self.tuning_factors = None
        self.unscaled_sums = None
        self.n_synthetic_apps = None

    def from_synthetic_workload(self, synth_workload):
        """
        Creates a ksf handler from synthetic workload data
        :param synth_workload:
        :return:
        """
        self.synth_app_data = synth_workload.app_list
        self.tuning_factors = synth_workload.get_tuning_factors()
        self.unscaled_sums = synth_workload.total_metrics_dict()
        self.n_synthetic_apps = len(synth_workload.app_list)
        return self

    def from_ksf_file(self, ksf_filename):

        # read the synthetic workload in the output folder
        with open(ksf_filename) as json_file:
            ksf_json_data = json.load(json_file)

        self.synth_app_json_data = ksf_json_data['jobs']
        self.tuning_factors = ksf_json_data['tuning_factors']
        self.unscaled_sums = ksf_json_data['unscaled_metrics_sums']
        self.n_synthetic_apps = len(ksf_json_data['jobs'])
        return self

    def export(self, filename, nbins):
        """
        Write a KSF file that describes the synthetic schedule,
        this file can be given directly to the executor
        :return:
        """

        if not filename.endswith('.ksf'):
            print("extension .ksp will be appended")
            filename += '.ksp'

        print_colour("white", "exporting ksf file: {}".format(filename))
        # retrieve entries from each synthetic app
        if self.synth_app_data:
            sorted_apps = sorted(self.synth_app_data, key=lambda a: a.time_start)
            synth_app_data_export = []
            for i, app in enumerate(sorted_apps):
                synth_app_data_export.append(app.export(filename, nbins, job_entry_only=True))
        else:
            if nbins > len(self.synth_app_json_data[0]['frames']):
                print_colour("orange", "exporting with more bins that the stored synthetic apps bins")
            synth_app_data_export = self.synth_app_json_data

        # pack the ksf info
        ksf_fields = {
            'unscaled_metrics_sums': self.unscaled_sums,
            'tuning_factors': self.tuning_factors,
            'jobs': synth_app_data_export
        }

        with open(filename, 'w') as f:
            json.dump(ksf_fields, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

        self.print_statistics()

    @property
    def scaled_sums(self):
        """
        REturn the scaled sums..
        :return:
        """
        scaled_sums = {}
        for k in self.unscaled_sums.keys():
            scaled_sums[k] = self.unscaled_sums[k]*self.tuning_factors[k]
        return scaled_sums

    def set_tuning_factors(self, tuning_factors):
        """
        Set the tuning factors
        :param tuning_factors:
        :return:
        """
        self.tuning_factors = tuning_factors

    def print_statistics(self):
        """
        Just print sums of synthetic apps
        """
        if not self.n_synthetic_apps:
            raise ValueError("Synthetic apps jsons not processed.. ")
        else:
            print "---------------------------------------------------------"
            print "Total number of synthetic apps = {}".format(self.n_synthetic_apps)

            print "\n---------- Sums of UNSCALED metrics: ------------------\n"
            for ss in time_signal.time_signal_names:
                print "    {} = {}".format(ss, self.unscaled_sums[ss])

            print "\n---------- Sums of SCALED metrics: ------------------\n"
            for ss in time_signal.time_signal_names:
                print "    {} = {}".format(ss, self.scaled_sums[ss])
