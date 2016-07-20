import numpy as np
from pylab import *
from matplotlib import dates

from datetime import datetime
from tools import *
from plot_handler import PlotHandler
from jobs import IngestedJob

from time_signal import TimeSignal
from logreader import scheduler_reader
from logreader import profiler_reader
from workload_corrector import WorkloadCorrector


class RealWorkload(object):
    """ Class main functionalities
        1) Contains a raw workload data
        2) Calculate derived data (e.g. app runtime, etc..)
        3) Display relevant statistics
    """

    def __init__(self, config_options):

        self.config_options = config_options
        self.scheduler_jobs = []
        self.profiler_jobs = []
        self.LogData = []

        self.input_dir = config_options.DIR_INPUT
        self.out_dir = config_options.DIR_OUTPUT
        self.total_metrics_nbins = config_options.REALWORKLOAD_TOTAL_METRICS_NBINS

        self.dir_input = config_options.DIR_INPUT
        self.job_output_all = np.array([]).reshape(3, 0)
        self.job_list = []

        # total metrics and parameters
        self.total_metrics = []
        self.minStartTime = None
        self.maxStartTime = None
        self.maxStartTime_fromT0 = None

        # parameters for the correction part..
        self.Ntime = config_options.WORKLOADCORRECTOR_NTIME
        self.Nfreq = config_options.WORKLOADCORRECTOR_NFREQ
        self.jobs_n_bins = config_options.WORKLOADCORRECTOR_JOBS_NBINS
        self.job_signal_names = [i_ts[0] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_type  = [i_ts[1] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_group = [i_ts[2] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_dgt   = [i_ts[3] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]

        # plots settings
        self.plot_tag = ""
        self.plot_time_tick = ""

    def read_logs(self,
                  scheduler_tag="",
                  profiler_tag="",
                  scheduler_log_file="",
                  profiler_log_dir="",
                  list_json_files=None
                  ):

        """ Read jobs from scheduler and profiler logs"""

        if scheduler_tag == "pbs":
            self.scheduler_jobs = scheduler_reader.read_pbs_logs(scheduler_log_file)
            scheduler_reader.make_scheduler_plots(self.scheduler_jobs,
                                                  self.plot_tag,
                                                  self.out_dir,
                                                  date_ticks=self.plot_time_tick)
        elif scheduler_tag == "accounting":
            self.scheduler_jobs = scheduler_reader.read_accounting_logs(scheduler_log_file)
            scheduler_reader.make_scheduler_plots(self.scheduler_jobs,
                                                  self.plot_tag,
                                                  self.out_dir,
                                                  date_ticks=self.plot_time_tick)
        else:
            self.scheduler_jobs = []
            print "scheduler jobs not found.."

        if profiler_tag == "allinea":
            self.profiler_jobs = profiler_reader.read_allinea_logs(profiler_log_dir, self.jobs_n_bins, list_json_files)
            self.LogData = self.profiler_jobs
        else:
            self.profiler_jobs = []
            raise ValueError(' Profiler jobs have NOT been found!')

        # Use the corrector to share job profiles to scheduler_tag jobs
        if self.scheduler_jobs and self.profiler_jobs:
            corrector = WorkloadCorrector(self.config_options)
            corrector.train_surrogate_model("ANN", self.profiler_jobs)
            corrector.ann_visual_check("Surrogate-model-test")
            self.scheduler_jobs = corrector.apply_surrogate_model(self.scheduler_jobs)
            self.LogData.extend(self.scheduler_jobs)

        # calculate the derived quantities
        self.calculate_global_metrics()

        # check that all the info are correctly filled in..
        for i_job in self.LogData:
            i_job.check_job()

    def calculate_global_metrics(self):
        """ Calculate all the relevant global metrics from the totals """

        self.minStartTime = min([i_job.time_start for i_job in self.LogData])
        self.maxStartTime = max([i_job.time_start for i_job in self.LogData])
        self.maxStartTime_fromT0 = self.maxStartTime - self.minStartTime

        # NOTE: this assumes that all the jobs have the same number and names of Time signals
        n_ts_in_job = len(self.LogData[0].timesignals)
        names_ts_in_job = [i_ts.name for i_ts in self.LogData[0].timesignals]
        ts_types = [i_ts.ts_type for i_ts in self.LogData[0].timesignals]
        ts_groups = [i_ts.ts_group for i_ts in self.LogData[0].timesignals]

        # aggregates all the signals (absolute times from T0)
        times_bins = np.asarray([item+i_job.time_start_0 for i_job in self.LogData for item in i_job.timesignals[0].xvalues_bins])
        for i_ts in range(0, n_ts_in_job):
            name_ts = 'total_' + names_ts_in_job[i_ts]
            yvals = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[i_ts].yvalues_bins])
            ts = TimeSignal()
            ts.create_ts_from_values(name_ts, ts_types[i_ts], ts_groups[i_ts], times_bins, yvals)
            self.total_metrics.append(ts)

        for (tt, i_tot) in enumerate(self.total_metrics):
            i_tot.digitize(self.total_metrics_nbins, self.job_signal_dgt[tt])

        # calculate relative impact factors (0 to 1)....
        imp_fac_all = [iJob.job_impact_index for iJob in self.LogData]
        max_diff = max(imp_fac_all) - min(imp_fac_all)
        if len(imp_fac_all) > 1:
            if max_diff:
                for iJob in self.LogData:
                    iJob.job_impact_index_rel = (iJob.job_impact_index - min(imp_fac_all)) / max_diff
            else:
                for iJob in self.LogData:
                    iJob.job_impact_index_rel = 0.0

    def make_plots(self, plot_tag):
        """ Make plots"""

        # scheduler specific plots..
        if self.scheduler_jobs:
            scheduler_reader.make_scheduler_plots(self.scheduler_jobs, plot_tag, self.out_dir)

        # WL total metrics plots
        i_fig = PlotHandler.get_fig_handle_ID()
        plt.figure(i_fig, figsize=(12, 20), dpi=80, facecolor='w', edgecolor='k')
        plt.title('global metrics')

        for (cc, i_tS) in enumerate(self.total_metrics):
            plt.subplot(len(self.total_metrics), 1, cc + 1)
            plt.bar(i_tS.xedge_bins[:-1], i_tS.yvalues_bins, i_tS.dx_bins, color='g')
            plt.plot(i_tS.xvalues, i_tS.yvalues, 'b.')
            plt.legend(['single jobs', 'sum'], loc=2)
            plt.ylabel(i_tS.name)
            # plt.yscale('log')

        plt.savefig(self.out_dir + '/' + plot_tag + '_plot_' + 'raw_global_data' + '.png')
        plt.close(i_fig)

        # def enrich_data_with_ts(self, user_key):
        #     """ Enrich data with time series """
        #
        #     if user_key == "FFT":
        #
        #         for iJob in self.LogData:
        #
        #             iJob.time_from_t0_vec = np.linspace(0, iJob.runtime, self.Ntime) + iJob.time_start_0
        #
        #             for i_ts in range(0, len(self.job_signal_names)):
        #                 freqs = np.random.random((self.Nfreq,)) * 1. / iJob.runtime * 10.
        #                 ampls = np.random.random((self.Nfreq,)) * 1000
        #                 phases = np.random.random((self.Nfreq,)) * 2 * np.pi
        #
        #                 sig_name = self.job_signal_names[i_ts]
        #                 sig_type = self.job_signal_type[i_ts]
        #                 sig_group = self.job_signal_group[i_ts]
        #
        #                 ts = TimeSignal()
        #                 ts.create_ts_from_spectrum(sig_name, sig_type, sig_group, iJob.time_from_t0_vec, freqs, ampls,
        #                                            phases)
        #                 iJob.append_time_signal(ts)
        #
        #         # NOTE: this assumes that all the jobs have the same number and
        #         # names of Time signals
        #         n_ts_in_job = len(self.LogData[0].timesignals)
        #         names_ts_in_job = [i_ts.name for i_ts in self.LogData[0].timesignals]
        #
        #         # aggregates all the signals
        #         total_time = np.asarray([item for iJob in self.LogData for item in iJob.time_from_t0_vec])
        #
        #         # loop over ts signals of each job
        #         for i_ts in range(0, n_ts_in_job):
        #             name_ts = 'total_' + names_ts_in_job[i_ts]
        #             values = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[i_ts].yvalues])
        #             ts = TimeSignal()
        #             ts.create_ts_from_values(name_ts, total_time, values)
        #             self.total_metrics.append(ts)
        #
        #     elif user_key == "bins":
        #
        #         for iJob in self.LogData:
        #
        #             iJob.time_from_t0_vec = np.linspace(0, iJob.runtime, self.Ntime) + iJob.time_start_0
        #
        #             for i_ts in range(0, len(self.job_signal_names)):
        #                 freqs = np.random.random((self.Nfreq,)) * 1. / iJob.runtime * 10.
        #                 ampls = np.random.random((self.Nfreq,)) * 1000
        #                 phases = np.random.random((self.Nfreq,)) * 2 * np.pi
        #
        #                 sig_name = self.job_signal_names[i_ts]
        #                 sig_type = self.job_signal_type[i_ts]
        #                 sig_group = self.job_signal_group[i_ts]
        #
        #                 ts = TimeSignal()
        #                 ts.create_ts_from_spectrum(sig_name, sig_type, sig_group, iJob.time_from_t0_vec, freqs, ampls,
        #                                            phases)
        #                 ts.digitize(self.jobs_n_bins, 'mean')
        #                 iJob.append_time_signal(ts)
        #
        #         # NOTE: this assumes that all the jobs have the same number and names of Time signals
        #         n_ts_in_job = len(self.LogData[0].timesignals)
        #         names_ts_in_job = [i_ts.name for i_ts in self.LogData[0].timesignals]
        #         ts_types = [i_ts.ts_type for i_ts in self.LogData[0].timesignals]
        #         ts_groups = [i_ts.ts_group for i_ts in self.LogData[0].timesignals]
        #
        #         # aggregates all the signals
        #         times_bins = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[0].xvalues_bins])
        #         for i_ts in range(0, n_ts_in_job):
        #             name_ts = 'total_' + names_ts_in_job[i_ts]
        #             yvals = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[i_ts].yvalues_bins])
        #             ts = TimeSignal()
        #             ts.create_ts_from_values(name_ts, ts_types[i_ts], ts_groups[i_ts], times_bins, yvals)
        #             self.total_metrics.append(ts)
        #
        #     else:
        #
        #         raise ValueError('option not recognised!')
