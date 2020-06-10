# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import numpy as np
from kronos_modeller.workload_modelling.time_schedule.time_schedule import TimeScheduleBase


class TimeSchedulePDFexact(TimeScheduleBase):
    """
    This class generate a time time_schedule for the model jobs according to the PDF of the submit times
    """

    def __init__(self,
                 start_times,
                 global_t0,
                 global_tend,
                 total_submit_interval,
                 submit_rate_factor,
                 n_bins_for_pdf):

        super(TimeSchedulePDFexact, self).__init__(start_times,
                                                  global_t0,
                                                  global_tend,
                                                  total_submit_interval,
                                                  submit_rate_factor,
                                                  n_bins_for_pdf)

    def create_schedule(self):
        """
        Function that returns a distribution of times according to a given PDF
        """

        assert (min(self.start_times) >= self.global_t0)
        assert (max(self.start_times) <= self.global_tend)

        eps_for_triggering_distribution = 1.0e-8

        # add an artificial small delta across t0_min and t0_max
        if self.global_tend == self.global_t0:
            self.global_tend += 1.0e-10

        # find the PDF of jobs start times
        input_time_min = min(self.start_times)
        input_time_max = max(self.start_times)+1.0e-10
        input_time_duration = float(input_time_max - input_time_min)
        input_time_duration_rel = input_time_duration / float(self.global_tend - self.global_t0)
        input_time_after_t0_rel = (input_time_min - self.global_t0) / float(self.global_tend - self.global_t0)
        input_time_bins = np.linspace(input_time_min, input_time_max, self.n_bins_for_pdf + 1)
        input_time_pdf, _ = np.histogram(self.start_times, input_time_bins, density=False)

        # then calculate an "exact" distribution of time start from the provided PDF
        output_time_pdf = input_time_pdf
        output_time_bins_01 = (input_time_bins - min(input_time_bins)) / float(max(input_time_bins) - min(input_time_bins))
        output_time_bins = (output_time_bins_01 * input_time_duration_rel + input_time_after_t0_rel) * self.total_submit_interval

        output_times = np.asarray([])
        output_times_pdf_actual = np.zeros(self.n_bins_for_pdf, dtype=int)

        # NOTE: Apply the algorithm based on the time distribution
        # only if total Dt > eps_for_triggering_distribution
        if self.global_tend - self.global_t0 > eps_for_triggering_distribution:

            for bb in range(self.n_bins_for_pdf):
                y_min = output_time_bins[bb]
                y_max = output_time_bins[bb + 1]

                # number of sa in this bin will depend of out/input time ratio and also desired submit-ratio
                # print "int(round(output_time_pdf[bb]*output_ratio * output_duration/input_time_duration))",
                # int(round(output_time_pdf[bb]*output_ratio * output_duration/input_time_duration))
                n_sa_bin = int(round(output_time_pdf[bb] * self.submit_rate_factor * self.total_submit_interval / float(self.global_tend - self.global_t0)))

                # this is the vector of actual random values of start times
                random_y_values = y_min + np.random.rand(n_sa_bin) * (y_max - y_min)

                # and now append them..
                output_times = np.append(output_times, random_y_values)
                output_times_pdf_actual[bb] = n_sa_bin

            # check if the output synthetic workload contains at least one synth app,
            # if not, add one in the most probable bin
            total_n_sa = sum(output_times_pdf_actual)
            if total_n_sa == 0:
                idx_max_pdf = np.argmax(input_time_pdf)
                y_min = output_time_bins[idx_max_pdf]
                y_max = output_time_bins[idx_max_pdf + 1]
                output_times = np.append(output_times, np.random.rand() * (y_max - y_min))
                output_times_pdf_actual[np.argmax(input_time_pdf)] = 1

        else:

            print("NOTE: the rate-of-submission algorithm is not used as the " \
                  "original rate is too high (t0_min: {}, t0_max: {})".format(self.global_t0,
                                                                              self.global_tend))
            output_times = np.zeros(len(self.start_times))
            output_times_pdf_actual = np.zeros(self.n_bins_for_pdf, dtype=int)

        return output_times, output_times_pdf_actual, output_time_bins
