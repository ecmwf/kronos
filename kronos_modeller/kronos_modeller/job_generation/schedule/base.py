# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


class TimeScheduleBase(object):
    """
    This class generate a time schedule for the model jobs
    """

    def __init__(self, start_times, global_t0, global_tend, total_submit_interval, submit_rate_factor, n_bins_for_pdf):
        self.start_times = start_times
        self.global_t0 = global_t0
        self.global_tend = global_tend
        self.total_submit_interval = total_submit_interval
        self.submit_rate_factor = submit_rate_factor
        self.n_bins_for_pdf = n_bins_for_pdf

    def create_schedule(self):
        """
        Function to create a time schedule for the jobs
        :return:
        """
        raise NotImplementedError("Not implemented attribute in base class..")
