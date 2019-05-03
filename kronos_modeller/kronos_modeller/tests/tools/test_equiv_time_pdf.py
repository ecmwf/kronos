#!/usr/bin/env python
# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest

import numpy as np
from kronos_modeller.workload_modelling.time_schedule import job_schedule_factory


class EquivTimePDFTest(unittest.TestCase):

    def test_equiv_time_pdf_exact(self):

        np.random.seed(0)

        n_bins = 10
        n_modelled_jobs = 50

        # global times
        global_t0 = 2.0
        global_tend = 100.0

        # input times
        input_times_min = 10.
        input_times_max = 30.
        start_times_vec = input_times_min + np.random.rand(n_modelled_jobs) * (input_times_max - input_times_min)
        input_hist, input_bins = np.histogram(start_times_vec, bins=n_bins)

        # output (rescaled) times
        output_duration = 98.0
        output_ratio = 1.0

        output_times, output_time_pdf, output_time_bins = job_schedule_factory["equiv_time_pdf_exact"](start_times_vec,
                                                                                                       global_t0,
                                                                                                       global_tend,
                                                                                                       output_duration,
                                                                                                       output_ratio,
                                                                                                       n_bins).create_schedule()

        # make sure that if the ratio is 1, it returns the same pdf
        self.assertTrue(all(input_hist == output_time_pdf))

        # # ------------------------------ plot ------------------------------
        # plot_handler = PlotHandler()
        # plt.figure(plot_handler.get_fig_handle_ID())
        # plt.subplot(2, 1, 1)
        # plt.bar((input_bins[1:]+input_bins[:-1])/2., input_hist, input_bins[1]-input_bins[0], color='b')
        # plt.xlim(xmin=global_t0, xmax=global_tend)
        # plt.xlabel('start time')
        # plt.ylabel('# jobs')
        # plt.subplot(2, 1, 2)
        # plt.bar((output_time_bins[1:]+output_time_bins[:-1])/2.,
        #         output_time_pdf, output_time_bins[1] - output_time_bins[0],
        #         color='r')
        # plt.xlim(xmin=0, xmax=output_duration)
        # plt.xlabel('start time')
        # plt.ylabel('# jobs')
        # plt.show()
        # # -----------------------------------------------------------------

    def test_equiv_time_pdf(self):

        np.random.seed(0)

        n_bins = 10
        n_modelled_jobs = 50

        # global times
        global_t0 = 2.0
        global_tend = 100.0

        # input times
        input_times_min = 10.
        input_times_max = 30.
        start_times_vec = input_times_min + np.random.rand(n_modelled_jobs) * (input_times_max - input_times_min)
        input_hist, input_bins = np.histogram(start_times_vec, bins=n_bins)

        # output (rescaled) times
        output_duration = 0.5
        output_ratio = (input_times_max-input_times_min)/output_duration

        output_times, output_time_pdf, output_time_bins = job_schedule_factory["equiv_time_pdf"](start_times_vec,
                                                                                                 global_t0,
                                                                                                 global_tend,
                                                                                                 output_duration,
                                                                                                 output_ratio,
                                                                                                 n_bins).create_schedule()

        # tests on the time stamps
        self.assertTrue((max(output_times)-min(output_times)) <= output_duration)

        # # ------------------------------ plot ------------------------------
        # plot_handler = PlotHandler()
        # plt.figure(plot_handler.get_fig_handle_ID())
        # plt.subplot(2, 1, 1)
        # plt.bar((input_bins[1:] + input_bins[:-1]) / 2., input_hist, input_bins[1]-input_bins[0], color='b')
        # plt.xlim(xmin=global_t0, xmax=global_tend)
        # plt.xlabel('start time')
        # plt.ylabel('# jobs')
        # plt.subplot(2, 1, 2)
        # plt.bar((output_time_bins[1:] + output_time_bins[:-1]) / 2.,
        #         output_time_pdf, output_time_bins[1] - output_time_bins[0],
        #         color='r')
        # plt.xlim(xmin=0, xmax=output_duration)
        # plt.xlabel('start time')
        # plt.ylabel('# jobs')
        # plt.show()
        # # -----------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
