import numpy as np
from pylab import *

from RealWorkload import RealWorkload

from pybrain.tools.shortcuts import buildNetwork
from pybrain.structure import FeedForwardNetwork
from pybrain.structure import LinearLayer, SigmoidLayer
from pybrain.structure import FullConnection

from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer

from tools import *
from PlotHandler import PlotHandler
from TimeSignal import TimeSignal


class WorkloadCorrector(object):

    """Class that enriches a workload with additional
       data and calculate global metrics
      1) tries to guess missing data (by regression methods)
      2) add time series wherever applicable
      3) calculate global metrics of the workload
      4) visualixe data
    """

    #================================================================
    def __init__(self, Workload, ConfigOptions):

        self.Workload = Workload
        self.out_dir = ConfigOptions.DIR_OUTPUT
        self.Ntime = ConfigOptions.WORKLOADCORRECTOR_NTIME
        self.Nfreq = ConfigOptions.WORKLOADCORRECTOR_NFREQ

        self.ANN_NNeurons = ConfigOptions.WORKLOADCORRECTOR_ANN_NNEURONS
        self.learningrate = ConfigOptions.WORKLOADCORRECTOR_ANN_LEARNINGRATE
        self.momentum = ConfigOptions.WORKLOADCORRECTOR_ANN_MOMENTUM
        self.weightdecay = ConfigOptions.WORKLOADCORRECTOR_ANN_WEIGHTDECAY
        self.splitRatio = ConfigOptions.WORKLOADCORRECTOR_ANN_SPLITRATIO
        self.epochs = ConfigOptions.WORKLOADCORRECTOR_ANN_EPOCHS

        self.Jobs_Nbins = ConfigOptions.WORKLOADCORRECTOR_JOBS_NBINS
        self.job_signal_names = ConfigOptions.WORKLOADCORRECTOR_LIST_TIME_NAMES

        self.Workload.total_metrics = []

    #================================================================
    def replace_missing_data(self, method_key):

        if (method_key == "ANN"):

            #--- TODO: check for missing data and replace them with synthetic values..
            #-- for the time being only an example is provided with runtime = runtime(ncpus)
            # also, still needs proper modular implementation (regression folder)

            #---------------- apply ANN on PBS data --------------------
            self.net = buildNetwork(1, self.ANN_NNeurons, 1, bias=True)

            # -------- Define ANN ----------
            #net = FeedForwardNetwork()
            #inLayer = LinearLayer(1)
            #hiddenLayer = SigmoidLayer(50)
            #outLayer = LinearLayer(1)
            # net.addInputModule(inLayer)
            # net.addModule(hiddenLayer)
            # net.addOutputModule(outLayer)
            #in_to_hidden = FullConnection(inLayer, hiddenLayer)
            #hidden_to_out = FullConnection(hiddenLayer, outLayer)
            # net.sortModules()
            # -----------------------------

            #---------- dataset ----------
            DS = SupervisedDataSet(1, 1)
            maxCPU = max([iJob.ncpus for iJob in self.Workload.LogData])
            maxRunTime = max([iJob.runtime for iJob in self.Workload.LogData])

            inputDataList = [iJob.ncpus for iJob in self.Workload.LogData]
            outputDataList = [iJob.runtime / float(maxRunTime) for iJob in self.Workload.LogData]
            
            self.ANN_normalizing_factor = float(maxRunTime)

            for iJob in range(0, len(outputDataList)):
                DS.appendLinked(inputDataList[iJob], outputDataList[iJob])

            self.TrainDS, self.TestDS = DS.splitWithProportion(self.splitRatio)

            trainer = BackpropTrainer(self.net,
                                      self.TrainDS,
                                      learningrate=self.learningrate,
                                      momentum=self.momentum,
                                      verbose=True,
                                      weightdecay=self.weightdecay)

            errlist = trainer.trainEpochs(epochs=self.epochs)
            #-----------------------------

    #================================================================
    def plot_missing_data(self, Tag):

        iFig = PlotHandler.get_fig_handle_ID()

        figure(iFig)
        xs = array([i for i in self.TestDS.getField('input')])
        ys = array([i for i in self.TestDS.getField('target')])

        idxs = np.argsort(xs, axis=0)
        xs = xs[idxs[:, 0]]
        ys = ys[idxs[:, 0]]

        ann_response = self.net.activateOnDataset(self.TestDS)

        ann_response = reshape(ann_response[idxs], (len(ann_response), 1))

        plot(xs, ys * self.ANN_normalizing_factor, 'bo')
        plot(xs, ann_response * self.ANN_normalizing_factor, 'r-')

        ylabel('Job runtimes [s]')
        xlabel('N CPU''s')
        legend(['target', 'ANN response'])
        savefig(self.out_dir + '/' + Tag + '_plot_' +'missing_data' + '_ncpu_runtime' + '.png')
        close(iFig)

    #================================================================
    # enrich data with time series
    def enrich_data_with_TS(self, user_key):

        if (user_key == "FFT"):

            for iJob in self.Workload.LogData:

                iJob.time_from_t0_vec = linspace(
                    0, iJob.runtime, self.Ntime) + iJob.time_start_0

                for iTS in range(0, len(self.job_signal_names)):

                    freqs = np.random.random((self.Nfreq,)) * 1. / iJob.runtime * 10.
                    ampls = np.random.random((self.Nfreq,)) * 1000
                    phases = np.random.random((self.Nfreq,)) * 2 * pi

                    sig_name = self.job_signal_names[iTS]
                    TS = TimeSignal()
                    TS.create_ts_from_spectrum(sig_name, iJob.time_from_t0_vec, freqs, ampls, phases)
                    iJob.append_time_signal(TS)

            # NOTE: this assumes that all the jobs have the same number and
            # names of Time signals
            NB_TS_eachJob = len(self.Workload.LogData[0].timesignals)
            Names_TS_eachJob = [iN.name for iN in self.Workload.LogData[0].timesignals]

            #----- aggregates all the signals -------
            total_time = np.asarray([item for iJob in self.Workload.LogData for item in iJob.time_from_t0_vec])

            #---- loop over TS signals of each job -----
            for iTS in range(0, NB_TS_eachJob):
                name_TS = 'total_' + Names_TS_eachJob[iTS]
                vals = np.asarray([item for iJob in self.Workload.LogData for item in iJob.timesignals[iTS].yvalues])
                TS = TimeSignal()
                TS.create_ts_from_values(name_TS, total_time, vals)
                self.Workload.total_metrics.append(TS)

        elif (user_key == "bins"):

            for iJob in self.Workload.LogData:

                iJob.time_from_t0_vec = linspace(0, iJob.runtime, self.Ntime) + iJob.time_start_0

                for iTS in range(0, len(self.job_signal_names)):
                    freqs = np.random.random((self.Nfreq,)) * 1. / iJob.runtime * 10.
                    ampls = np.random.random((self.Nfreq,)) * 1000
                    phases = np.random.random((self.Nfreq,)) * 2 * pi

                    sig_name = self.job_signal_names[iTS]
                    TS = TimeSignal()
                    TS.create_ts_from_spectrum(sig_name, iJob.time_from_t0_vec, freqs, ampls, phases)
                    TS.digitize(self.Jobs_Nbins, 'mean')
                    iJob.append_time_signal(TS)

            # NOTE: this assumes that all the jobs have the same number and
            # names of Time signals
            NB_TS_eachJob = len(self.Workload.LogData[0].timesignals)
            Names_TS_eachJob = [iN.name for iN in self.Workload.LogData[0].timesignals]

            #----- aggregates al the signals -------
            times_bins = np.asarray([item for iJob in self.Workload.LogData for item in iJob.timesignals[0].xvalues_bins])
            for iTS in range(0, NB_TS_eachJob):
                name_TS = 'total_' + Names_TS_eachJob[iTS]
                yvals = np.asarray([item for iJob in self.Workload.LogData for item in iJob.timesignals[iTS].yvalues_bins])
                TS = TimeSignal()
                TS.create_ts_from_values(name_TS, times_bins, yvals)
                self.Workload.total_metrics.append(TS)

            #from IPython.core.debugger import Tracer
            # Tracer()()

        else:

            raise ValueError('option not recognised!')

    #================================================================
    #-- calsulate all the relevant global metrics from the totals
    def calculate_global_metrics(self):

        print "in calculate_global_metrics"

        for iTot in self.Workload.total_metrics:
            iTot.digitize(10, 'sum')

        #----- calculate relative impact factors (0 to 1)....
        imp_fac_all = [iJob.job_impact_index() for iJob in self.Workload.LogData]
        for iJob in self.Workload.LogData:
            iJob.job_impact_index_rel = (iJob.job_impact_index() - min(imp_fac_all)) / (max(imp_fac_all) - min(imp_fac_all))

    #================================================================
    def make_plots(self, Tag):

        iFig = PlotHandler.get_fig_handle_ID()
        figure(iFig, figsize=(12, 18), dpi=80, facecolor='w', edgecolor='k')
        title('global metrics')

        for cc, iTS in enumerate(self.Workload.total_metrics):

            subplot(4, 1, cc + 1)

            bar(iTS.xedge_bins[:-1], iTS.yvalues_bins, iTS.DX_bins, color='g')
            plot(iTS.xvalues, iTS.yvalues, 'b.')

            legend(['single jobs', 'sum'], loc=2)
            ylabel(iTS.name)
            yscale('log')

        savefig(self.out_dir + '/' + Tag + '_plot_' + 'raw_global_data' + '.png')
        close(iFig)
        #----------------------------------------
