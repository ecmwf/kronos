import numpy as np
import matplotlib.pyplot as plt

from pybrain.tools.shortcuts import buildNetwork
from pybrain.datasets import SupervisedDataSet
from pybrain.datasets import UnsupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer

from plot_handler import PlotHandler
from time_signal import TimeSignal


class WorkloadCorrector(object):

    """Class that enriches a workload with additional
       data and calculate global metrics
      1) tries to guess missing data (by regression methods)
      2) add time series wherever applicable
      3) calculate global metrics of the workload
      4) visualize data
    """

    def __init__(self, config_param):

        # self.Workload = input_wl
        self.out_dir = config_param.DIR_OUTPUT
        self.ts_name_type = config_param.WORKLOADCORRECTOR_LIST_TIME_NAMES

        # ann configuration params
        self.eps = config_param.WORKLOADCORRECTOR_EPS
        self.ann_name_inputs = config_param.WORKLOADCORRECTOR_ANN_INPUT_NAMES
        self.ann_n_inputs = len(self.ann_name_inputs)
        self.ann_name_outputs = [i_name[0] for i_name in config_param.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_dgt = [i_ts[3] for i_ts in config_param.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.ann_n_outputs = len(self.ts_name_type)
        self.ann_n_neurons = config_param.WORKLOADCORRECTOR_ANN_NNEURONS
        self.ann_learning_rate = config_param.WORKLOADCORRECTOR_ANN_LEARNINGRATE
        self.ann_momentum = config_param.WORKLOADCORRECTOR_ANN_MOMENTUM
        self.ann_weight_decay = config_param.WORKLOADCORRECTOR_ANN_WEIGHTDECAY
        self.ann_split_ratio = config_param.WORKLOADCORRECTOR_ANN_SPLITRATIO
        self.ann_epochs = config_param.WORKLOADCORRECTOR_ANN_EPOCHS

        # ann initialization
        self.ann_normalizing_factor = None
        self.ann_net = None
        self.ann_dataset_train = None
        self.ann_dataset_test = None

        # bins in jobs
        self.jobs_n_bins = config_param.WORKLOADCORRECTOR_JOBS_NBINS

    def train_surrogate_model(self, method_key, profiler_jobs):

        if method_key == "ANN":

            # TODO: check for missing data and replace them with synthetic values..
            # needs proper modular implementation (regression folder)

            # Set-up ANN
            self.ann_net = buildNetwork(self.ann_n_inputs, self.ann_n_neurons, self.ann_n_outputs, bias=True)
            ann_dataset_all = SupervisedDataSet(self.ann_n_inputs, self.ann_n_outputs)

            ann_input_data = [[i_job.ncpus, float(i_job.time_end-i_job.time_start)] for i_job in profiler_jobs]
            ann_output_data = [[i_ts.sum for i_ts in i_job.timesignals] for i_job in profiler_jobs]

            # normalize the output
            ann_output_data_max = [max(row) for row in ann_output_data]
            self.ann_normalizing_factor = ann_output_data_max
            ann_output_data = [[i_c/ann_output_data_max[rr] for i_c in row] for (rr, row) in enumerate(ann_output_data)]

            for i_job in range(0, len(ann_output_data)):
                ann_dataset_all.appendLinked(ann_input_data[i_job], ann_output_data[i_job])

            self.ann_dataset_train, self.ann_dataset_test = ann_dataset_all.splitWithProportion(self.ann_split_ratio)

            trainer = BackpropTrainer(self.ann_net,
                                      self.ann_dataset_train,
                                      learningrate=self.ann_learning_rate,
                                      momentum=self.ann_momentum,
                                      verbose=True,
                                      weightdecay=self.ann_weight_decay)

            errlist = trainer.trainEpochs(epochs=self.ann_epochs)

    def ann_visual_check(self, plt_tag):
        """ Plot ANN response on testing set """

        in_vec = self.ann_dataset_test.getField('input')
        out_vec = self.ann_dataset_test.getField('target')
        ann_response = self.ann_net.activateOnDataset(self.ann_dataset_test)

        for i_in in range(0, self.ann_n_inputs):
            for i_out in range(0, self.ann_n_outputs):
                i_fig = PlotHandler.get_fig_handle_ID()
                plt.figure(i_fig)

                xs = in_vec[:, i_in]
                ys = out_vec[:, i_out]

                x_name = self.ann_name_inputs[i_in]
                y_name = self.ann_name_outputs[i_out]

                idxs = np.argsort(xs, axis=0)
                xs = xs[idxs]
                ys = ys[idxs]
                ann_response_y = ann_response[idxs, i_out]

                plt.plot(xs, [yy * self.ann_normalizing_factor[ii] for (ii, yy) in enumerate(ys)], 'bo')
                plt.plot(xs, [ann_response_y[ii] * self.ann_normalizing_factor[ii] for (ii, yy) in enumerate(ys)], 'r-')

                plt.xlabel(x_name)
                plt.ylabel(y_name)
                plt.legend(['target', 'ANN response'])
                plt.savefig(self.out_dir + '/' + plt_tag + '_plot_' + 'in=' + x_name + '_out=' + y_name + '_missing_data' + '.png')
                plt.close(i_fig)

    def apply_surrogate_model(self, scheduler_jobs):
        """ Apply surrogate model onto scheduler jobs """

        ann_input_ds = UnsupervisedDataSet(self.ann_n_inputs)
        for i_job in scheduler_jobs:
            ann_input_ds.addSample([i_job.ncpus, float(i_job.time_end-i_job.time_start)])

        # activate ann on scheduler jobs
        ann_response = self.ann_net.activateOnDataset(ann_input_ds)

        # pad time signals for each scheduler jobs
        for (jj, i_job) in enumerate(scheduler_jobs):
            for (tt, i_ts_name) in enumerate(self.ann_name_outputs):

                ts = TimeSignal()

                dt = float(i_job.time_end-i_job.time_start)/self.jobs_n_bins
                sample_times = np.arange(0, self.jobs_n_bins)*dt+dt/2.0
                y_values = np.ones(self.jobs_n_bins)*max(self.eps, ann_response[jj][tt])

                ts_type = self.ts_name_type[tt][1]
                ts_ker_type = self.ts_name_type[tt][2]

                ts.create_ts_from_values(i_ts_name, ts_type, ts_ker_type, sample_times, y_values)
                ts.digitize(self.jobs_n_bins, self.job_signal_dgt[tt])
                i_job.append_time_signal(ts)

        return scheduler_jobs
