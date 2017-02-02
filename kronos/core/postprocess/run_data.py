# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import csv
import os

from kronos.core import time_signal
from kronos.core.exceptions_iows import ConfigurationError

from kronos.io.profile_format import ProfileFormat
from kronos.io.schedule_format import ScheduleFormat


class RunData(object):
    """
    This class loads up and analyses the results of a simulation
    """

    def __init__(self, path_root):

        self.path_root = path_root
        self.ksf_data = None
        self.log_data = None
        self.kpf_data = None
        self.n_iterations = None

    def get_ksf_data(self, iteration=None):
        """
        Get ksf data from a ksf file for a specified iteration
        :param iteration:
        :return:
        """

        iter_id = iteration if iteration else 0

        # find the ksf file in the run folder
        ksf_iteration_dir = os.path.join(self.path_root, 'iteration-{}/sa_jsons'.format(iter_id))
        ksf_files_in_dir = [fname for fname in os.listdir(ksf_iteration_dir) if fname.endswith(".ksf")]
        if len(ksf_files_in_dir) > 1:
            raise ConfigurationError("found more than one ksf file in run folder!")
        ksf_filename = os.path.join(ksf_iteration_dir, ksf_files_in_dir[0])

        return ScheduleFormat.from_filename(ksf_filename)

    def get_kpf_data(self, iteration=None):
        """
        Get kpf data from a file of for specified iteration
        :param iteration:
        :return:
        """

        iter_id = iteration if iteration else 0

        # find the kpf file in the run folder
        kpf_iteration_dir = os.path.join(self.path_root, 'iteration-{}/run_jsons'.format(iter_id))
        kpf_files_in_dir = [fname for fname in os.listdir(kpf_iteration_dir) if fname.endswith(".kpf")]
        if len(kpf_files_in_dir) > 1:
            raise ConfigurationError("found more than one ksf file in run folder!")
        kpf_filename = os.path.join(kpf_iteration_dir, kpf_files_in_dir[0])

        print "getting kpf data for iteration {}".format(iter_id)

        return ProfileFormat.from_filename(kpf_filename)

    def get_n_iterations(self):
        """
        Reads the number of iterations for this run
        :return:
        """

        iter_dir_list = [os.path.join(self.path_root, o) for o in os.listdir(self.path_root)
                         if os.path.isdir(os.path.join(self.path_root, o)) and o.startswith('iteration')]

        # number of iterations
        return len(iter_dir_list)

    def get_log_data(self):
        """
        Get the data from the log file of the run
        :return:
        """

        # log file in output folder
        log_file = os.path.join(self.path_root, 'log_file.txt')
        reader = csv.reader(open(log_file), delimiter=" ")
        reader_lines = [[n for n in ll if n is not ''] for ll in reader]
        header = reader_lines[0]
        iterations = reader_lines[1:]

        # build a dictionary with the iterations values
        iter_vec = range(0, len(iterations))
        log_data_dict = {}
        for tt, ts in enumerate(header):
            log_data_dict[ts] = [float(iterations[ii][tt]) for ii in iter_vec]

        return log_data_dict

    def print_schedule_summary(self, iteration=None):
        """
        Pritn statistics from a specified ksf file
        :param iteration:
        :return:
        """

        iter_id = iteration if iteration else 0

        # find the ksf file in the run folder
        ksf_iteration_dir = os.path.join(self.path_root, 'iteration-{}/sa_jsons'.format(iter_id))
        ksf_files_in_dir = [fname for fname in os.listdir(ksf_iteration_dir) if fname.endswith(".ksf")]
        if len(ksf_files_in_dir) > 1:
            raise ConfigurationError("found more than one ksf file in run folder!")
        ksf_filename = os.path.join(ksf_iteration_dir, ksf_files_in_dir[0])

        print "getting ksf data for iteration {}".format(iter_id)

        # KSFFileHandler().from_ksf_file(ksf_filename).print_statistics()
        schedule = ScheduleFormat.from_filename(ksf_filename)

        if not schedule.sa_data_json:
            raise ValueError("Synthetic apps jsons not processed.. ")
        else:
            print "---------------------------------------------------------"
            print "Total number of synthetic apps = {}".format(len(schedule.sa_data_json))

            print "\n---------- Sums of UNSCALED metrics: ------------------\n"
            for ss in time_signal.time_signal_names:
                print "    {} = {}".format(ss, schedule.unscaled_sums[ss])

            print "\n---------- Sums of SCALED metrics: ------------------\n"
            for ss in time_signal.time_signal_names:
                print "    {} = {}".format(ss, schedule.scaled_sums[ss])
