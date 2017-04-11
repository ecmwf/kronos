# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import numpy as np

from kronos.core.job_generation.schedule.base import TimeScheduleBase


class TimeSchedulePDF(TimeScheduleBase):
    """
    This class generate a time schedule for the model jobs according to the PDF of the submit times
    """

    def __init__(self,
                 start_times,
                 global_t0,
                 global_tend,
                 total_submit_interval,
                 submit_rate_factor,
                 n_bins_for_pdf):

        super(TimeSchedulePDF, self).__init__(start_times,
                                              global_t0,
                                              global_tend,
                                              total_submit_interval,
                                              submit_rate_factor,
                                              n_bins_for_pdf)

    def create_schedule(self):
        """
        Function that returns a "random" distribution of times according to a given PDF
        """
        
        assert (min(self.start_times) >= self.global_t0)
        assert (max(self.start_times) <= self.global_tend)
        
        # calculate the submit rate from the selected workload
        real_submit_rate = float(len(self.start_times)) / (max(self.start_times) - min(self.start_times))
        requested_submit_rate = real_submit_rate * self.submit_rate_factor
        n_modelled_jobs = max(1, int(requested_submit_rate * self.total_submit_interval))
        
        # find the PDF of jobs start times
        input_time_min = min(self.start_times)
        input_time_max = max(self.start_times)
        input_time_duration_rel = (input_time_max - input_time_min) / (self.global_tend - self.global_t0)
        input_time_after_t0_rel = (input_time_min - self.global_t0) / (self.global_tend - self.global_t0)
        input_time_bins = np.linspace(input_time_min, input_time_max, self.n_bins_for_pdf + 1)
        # input_time_bins_mid = (input_time_bins[:-1] + input_time_bins[1:]) / 2.0
        
        input_time_pdf, _ = np.histogram(self.start_times, input_time_bins, density=False)
        
        # then calculate an "exact" distribution of time start from the provided PDF
        output_time_pdf = input_time_pdf
        output_time_bins_01 = (input_time_bins - min(input_time_bins)) / (max(input_time_bins) - min(input_time_bins))
        output_time_bins = (output_time_bins_01 * input_time_duration_rel + input_time_after_t0_rel) * self.total_submit_interval
        output_time_bins_mid = (output_time_bins[:-1] + output_time_bins[1:]) / 2.0
        
        output_times = np.random.choice(output_time_bins_mid, p=output_time_pdf / float(sum(output_time_pdf)),
                                        size=n_modelled_jobs)
        output_times_pdf_actual, _ = np.histogram(output_times, output_time_bins, density=False)
        
        # check if the output synthetic workload contains at least one synth app,
        # if not, add one in the most probable bin
        total_n_sa = sum(output_times_pdf_actual)
        if total_n_sa == 0:
            idx_max_pdf = np.argmax(input_time_pdf)
            y_min = output_time_bins[idx_max_pdf]
            y_max = output_time_bins[idx_max_pdf + 1]
            output_times = np.append(output_times, np.random.rand() * (y_max - y_min))
            output_times_pdf_actual[np.argmax(input_time_pdf)] = 1
        
        return output_times, output_times_pdf_actual, output_time_bins
