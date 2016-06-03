import numpy as np
import matplotlib.pyplot as plt

from pybrain.tools.shortcuts import buildNetwork
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer

from plot_handler import PlotHandler


class WorkloadCorrector(object):

    """Class that enriches a workload with additional
       data and calculate global metrics
      1) tries to guess missing data (by regression methods)
      2) add time series wherever applicable
      3) calculate global metrics of the workload
      4) visualize data
    """

    def __init__(self, input_wl, config_param):

        self.Workload = input_wl
        self.out_dir = config_param.DIR_OUTPUT

        self.net = None
        self.ANN_name_inputs = config_param.WORKLOADCORRECTOR_ANN_INPUT_NAMES
        self.ANN_NNeurons = config_param.WORKLOADCORRECTOR_ANN_NNEURONS
        self.learningrate = config_param.WORKLOADCORRECTOR_ANN_LEARNINGRATE
        self.momentum = config_param.WORKLOADCORRECTOR_ANN_MOMENTUM
        self.weightdecay = config_param.WORKLOADCORRECTOR_ANN_WEIGHTDECAY
        self.split_ratio = config_param.WORKLOADCORRECTOR_ANN_SPLITRATIO
        self.epochs = config_param.WORKLOADCORRECTOR_ANN_EPOCHS
        self.ANN_normalizing_factor = None
        self.ANN_n_outputs = len(config_param.WORKLOADCORRECTOR_LIST_TIME_NAMES)
        self.ANN_name_outputs = [i_name[0] for i_name in config_param.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.ann_dataset_train = None
        self.ann_dataset_test = None

    def replace_missing_data(self, method_key):

        if method_key == "ANN":

            # TODO: check for missing data and replace them with synthetic values..
            # for the time being only an example is provided with runtime = runtime(ncpus)
            # also, still needs proper modular implementation (regression folder)

            # Set-up ANN
            n_inputs = len(self.ANN_name_inputs)
            self.net = buildNetwork(n_inputs, self.ANN_NNeurons, self.ANN_n_outputs, bias=True)
            ann_dataset_all = SupervisedDataSet(3, self.ANN_n_outputs)

            ann_input_data = [[i_job.ncpus, i_job.runtime, i_job.time_in_queue ] for i_job in self.Workload.LogData]
            ann_output_data = [[i_ts.sum for i_ts in i_job.timesignals] for i_job in self.Workload.LogData]

            # normalize the output
            ann_output_data_max = [max(row) for row in ann_output_data]
            self.ANN_normalizing_factor = ann_output_data_max
            ann_output_data = [[i_c/ann_output_data_max[rr] for (rr, i_c) in enumerate(row)] for row in ann_output_data]

            for i_job in range(0, len(ann_output_data)):
                ann_dataset_all.appendLinked(ann_input_data[i_job], ann_output_data[i_job])

            self.ann_dataset_train, self.ann_dataset_test = ann_dataset_all.splitWithProportion(self.split_ratio)

            trainer = BackpropTrainer(self.net,
                                      self.ann_dataset_train,
                                      learningrate=self.learningrate,
                                      momentum=self.momentum,
                                      verbose=True,
                                      weightdecay=self.weightdecay)

            errlist = trainer.trainEpochs(epochs=self.epochs)

    def plot_missing_data(self, plt_tag):

        in_vec = self.ann_dataset_test.getField('input')
        out_vec = self.ann_dataset_test.getField('target')
        ann_response = self.net.activateOnDataset(self.ann_dataset_test)

        for i_in in range(0,3):
            for i_out in range(0, self.ANN_n_outputs):

                i_fig = PlotHandler.get_fig_handle_ID()
                plt.figure(i_fig)

                xs = in_vec[:, i_in]
                ys = out_vec[:, i_out]

                x_name = self.ANN_name_inputs[i_in]
                y_name = self.ANN_name_outputs[i_out]

                idxs = np.argsort(xs, axis=0)
                xs = xs[idxs]
                ys = ys[idxs]
                ann_response_y = ann_response[idxs, i_out]

                plt.plot(xs, [yy * self.ANN_normalizing_factor[ii] for (ii, yy) in enumerate(ys)], 'bo')
                plt.plot(xs, [ann_response_y[ii] * self.ANN_normalizing_factor[ii] for (ii, yy) in enumerate(ys)], 'r-')

                plt.xlabel(x_name)
                plt.ylabel(y_name)
                plt.legend(['target', 'ANN response'])
                plt.savefig(self.out_dir + '/' + plt_tag + '_plot_' + 'in=' + x_name + '_out=' + y_name + '_missing_data' + '.png')
                plt.close(i_fig)

    def make_plots(self, plt_tag):

        """ plotting routine """

        i_fig = PlotHandler.get_fig_handle_ID()
        plt.figure(i_fig, figsize=(12, 20), dpi=80, facecolor='w', edgecolor='k')
        plt.title('global metrics')

        for (cc, i_tS) in enumerate(self.Workload.total_metrics):
            plt.subplot(len(self.Workload.total_metrics), 1, cc + 1)
            plt.bar(i_tS.xedge_bins[:-1], i_tS.yvalues_bins, i_tS.dx_bins, color='g')
            plt.plot(i_tS.xvalues, i_tS.yvalues, 'b.')
            plt.legend(['single jobs', 'sum'], loc=2)
            plt.ylabel(i_tS.name)
            plt.yscale('log')

        plt.savefig(self.out_dir + '/' + plt_tag + '_plot_' + 'raw_global_data' + '.png')
        plt.close(i_fig)
