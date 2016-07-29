from pylab import *

import time_signal
from logreader.scheduler_reader import AccountingDataSet, PBSDataSet
from plot_handler import PlotHandler
from sds.print_colour import print_colour

from time_signal import TimeSignal
from logreader import scheduler_reader
from logreader import profiler_reader
from workload_corrector import WorkloadCorrector


class ModelWorkload(object):
    """ Class main functionalities
        1) Contains a raw workload data
        2) Calculate derived data (e.g. app runtime, etc..)
        3) Display relevant statistics
    """

    def __init__(self, config_options=None):

        self.config_options = config_options
        self.scheduler_jobs = []
        self.profiler_jobs = []
        self.job_list = []

        self.input_dir = config_options.dir_input
        self.out_dir = config_options.dir_output
        self.total_metrics_nbins = config_options.REALWORKLOAD_TOTAL_METRICS_NBINS

        self.dir_input = config_options.dir_input
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

        # plots settings
        self.plot_tag = ""
        self.plot_time_tick = ""

    def __unicode__(self):
        return "ModelWorkload - {} jobs".format(len(self.job_list))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def verbose_description(self):
        """
        A verbose output for the modelled workload
        """
        output = "=============================================\n"
        output += "Verbose workload description: {}\n".format(self)

        for i, job in enumerate(self.job_list):
            output += '--\nJob {}:\n'.format(i)
            output += job.verbose_description()

        output += "=============================================\n"
        return output

    def read_logs(self, scheduler_tag="", profiler_tag="", scheduler_log_file="", profiler_log_dir=""):
        """ Read jobs from scheduler and profiler logs"""

        ## TODO

        ## Remember to include the plot functionality

        datasets = []
        if scheduler_tag == "pbs":
            datasets.append(scheduler_reader.ingest_pbs_logs(scheduler_log_file))
        elif scheduler_tag == "accounting":
            datasets.append(scheduler_reader.ingest_accounting_logs(scheduler_log_file))
        else:
            print "Scheduler jobs not found"

        if profiler_tag == "allinea":
            datasets.append(profiler_reader.ingest_allinea_profiles(profiler_log_dir, self.jobs_n_bins))
        else:
            raise ValueError("No profiler jobs have been found")

        print "Ingested data sets: [\n" + ",\n".join(["    {}".format(d) for d in datasets]) +  "\n]"

        #scheduler_reader.make_scheduler_plots(self.scheduler_jobs,
        #                                      self.plot_tag,
        #                                      self.out_dir,
        #                                      date_ticks=self.plot_time_tick)

        self.model_ingested_datasets(datasets)

        # check that all the info are correctly filled in..
        for i_job in self.job_list:
            i_job.check_job()

    def model_ingested_datasets(self, datasets):
        """
        This is an (in principle) arbitrarily complex process. We need to transform a number of
        sets of jobs into one well-defined set of ModelJobs.

        For now, ASSERT that we only have data that has been brought in from Allinea.
        """

        # TODO: Consider the scheduler jobs first, then update the job list with information
        #       from the (various) profiling options that have taken place.
        #
        # n.b. We can only cope with _one_ Scheduling dataset.

        # For now, this is somewhat ugly. We can only deal with _one_ scheduler, and _one_
        # non-scheduler based dataset.

        if len(datasets) == 1:
            self.job_list = list(datasets[0].model_jobs())
        else:
            assert len(datasets) == 2

            if isinstance(datasets[0], PBSDataSet) or isinstance(datasets[0], AccountingDataSet):
                scheduler_dataset, profiler_dataset = datasets[0], datasets[1]
            else:
                scheduler_dataset, profiler_dataset = datasets[1], datasets[0]
            assert isinstance(scheduler_dataset, PBSDataSet) or isinstance(scheduler_dataset, AccountingDataSet)
            assert not (isinstance(profiler_dataset, PBSDataSet) or isinstance(profiler_dataset, AccountingDataSet))

            corrector = WorkloadCorrector(self.config_options)
            corrector.train_surrogate_model("ANN", list(profiler_dataset.model_jobs()))
            corrector.ann_visual_check("Surrogate-model-test")
            scheduler_jobs = corrector.apply_surrogate_model(list(scheduler_dataset.model_jobs()))
            self.job_list.extend(scheduler_jobs)

        # Ensure that the data is in a reasonable state for further processing
        #
        # It all needs to be sufficiently standardised.
        # TODO: We should maybe do this in ModelJobs? Or do the digitization on the fly?
        for job in self.job_list:
            for ts_name, ts in job.timesignals.iteritems():

                # If there is no data for a time series, fill it in with a blank
                if ts is None:
                    job.timesignals[ts_name] = TimeSignal.from_values(ts_name, [0.0], [0.0])
                    ts = job.timesignals[ts_name]

                if ts.xvalues_bins is None:
                    ts.digitize(self.config_options.WORKLOADCORRECTOR_JOBS_NBINS)

        self.calculate_global_metrics()

    def calculate_global_metrics(self):
        """ Calculate all the relevant global metrics from the totals """

        self.minStartTime = min([i_job.time_start for i_job in self.job_list])
        self.maxStartTime = max([i_job.time_start for i_job in self.job_list])
        self.maxStartTime_fromT0 = self.maxStartTime - self.minStartTime

        # Concatenate all the available time series data for each of the jobs

        for signal_name, signal_details in time_signal.signal_types.iteritems():

            try:
                times_bin = np.concatenate([job.timesignals[signal_name].xvalues_bins for job in self.job_list if job.timesignals[signal_name] is not None])
                data = np.concatenate([job.timesignals[signal_name].yvalues_bins for job in self.job_list if job.timesignals[signal_name] is not None])

                ts = TimeSignal.from_values('total_{}'.format(signal_name), times_bin, data, base_signal_name=signal_name)
                ts.digitize(self.total_metrics_nbins)
                self.total_metrics.append(ts)

            except ValueError:
                print_colour("orange", "No jobs found with time series for {}".format(signal_name))

        # # calculate relative impact factors (0 to 1)....
        imp_fac_all = [job.job_impact_index for job in self.job_list]
        max_diff = max(imp_fac_all) - min(imp_fac_all)
        if len(imp_fac_all) > 1:
            if max_diff:
                for iJob in self.job_list:
                    iJob.job_impact_index_rel = (iJob.job_impact_index - min(imp_fac_all)) / max_diff
            else:
                for iJob in self.job_list:
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
        #         for iJob in self.job_list:
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
        #         time_signals_per_job = len(self.job_list[0].timesignals)
        #         names_ts_in_job = [i_ts.name for i_ts in self.job_list[0].timesignals]
        #
        #         # aggregates all the signals
        #         total_time = np.asarray([item for iJob in self.job_list for item in iJob.time_from_t0_vec])
        #
        #         # loop over ts signals of each job
        #         for i_ts in range(0, time_signals_per_job):
        #             name_ts = 'total_' + names_ts_in_job[i_ts]
        #             values = np.asarray([item for iJob in self.job_list for item in iJob.timesignals[i_ts].yvalues])
        #             ts = TimeSignal()
        #             ts.create_ts_from_values(name_ts, total_time, values)
        #             self.total_metrics.append(ts)
        #
        #     elif user_key == "bins":
        #
        #         for iJob in self.job_list:
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
        #         time_signals_per_job = len(self.job_list[0].timesignals)
        #         names_ts_in_job = [i_ts.name for i_ts in self.job_list[0].timesignals]
        #         ts_types = [i_ts.ts_type for i_ts in self.job_list[0].timesignals]
        #         ts_groups = [i_ts.ts_group for i_ts in self.job_list[0].timesignals]
        #
        #         # aggregates all the signals
        #         times_bins = np.asarray([item for iJob in self.job_list for item in iJob.timesignals[0].xvalues_bins])
        #         for i_ts in range(0, time_signals_per_job):
        #             name_ts = 'total_' + names_ts_in_job[i_ts]
        #             yvals = np.asarray([item for iJob in self.job_list for item in iJob.timesignals[i_ts].yvalues_bins])
        #             ts = TimeSignal()
        #             ts.create_ts_from_values(name_ts, ts_types[i_ts], ts_groups[i_ts], times_bins, yvals)
        #             self.total_metrics.append(ts)
        #
        #     else:
        #
        #         raise ValueError('option not recognised!')
