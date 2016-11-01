import os
import glob
import json
import pprint
from time_signal import signal_types
from kronos_tools.print_colour import print_colour


class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.config = config

    def ingest_data(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def generate_model(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def run(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def postprocess(self):
        """
        Do some post-processing of the synthetic apps (modelled and run)
        :return:
        """

        config_pp = self.config.post_process

        # raise NotImplementedError("Must use derived class. Call clustering.factory")
        pp = pprint.PrettyPrinter(indent=4)

        # list of json files in folder..
        json_files = glob.glob(os.path.join(os.path.realpath(config_pp['dir_sa_run_input']), "*.json"))
        if not json_files:
            raise OSError("no json files of synth apps found in {}".format(config_pp['dir_sa_run_input']))

        json_files.sort()

        # ============== reformat the sa dictionary for easy use.. ==================
        sa_data_list = []
        for file_name in json_files:

            print_colour("green", "reading file {}".format(file_name))

            # Initialize the sa data
            sa_data = {}

            # Initialize frames
            sa_data['frames'] = {}
            for ss in signal_types.keys():
                sa_data['frames'][ss] = []

            # Read sa data from json file
            with open(file_name) as data_file:
                json_data = json.load(data_file)

            # read frame data and append to lists
            for ker_list in json_data['frames']:
                for ker in ker_list:
                    for k in ker.keys():
                        if k in signal_types:
                            sa_data['frames'][k].append(ker[k])

            # put zeros instead of empty frames
            for signal_name in sa_data['frames'].keys():
                if not sa_data['frames'][signal_name]:
                    sa_data['frames'][signal_name] = [0]

            # read the other keys..
            sa_data['num_procs'] = json_data['num_procs']
            sa_data['start_delay'] = json_data['start_delay']

            sa_data_list.append(sa_data)

        # # print out the sa list..
        # for sa in sa_data_list:
        #     pp.pprint(sa)

        print "--------------------- printing summary -------------------------"
        print "N synthetic apps = {}".format(len(sa_data_list))

        print "Sums of all quantities:"
        sums_sig = {el: 0 for el in signal_types}
        for sa in sa_data_list:
            for tt in sa['frames'].keys():
                sums_sig[tt] += sum(sa['frames'][tt])

        for ss in sums_sig.keys():
            print "    {} = {}".format(ss, sums_sig[ss])





