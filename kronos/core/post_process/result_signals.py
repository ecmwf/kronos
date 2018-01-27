# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import numpy as np
from kronos.core.post_process.exportable_types import ExportableSignal


class ResultSignal(object):
    """
    Result signal is a time signal that comes from parsing the logs of the simulation.
    It can be converted into an exportable signal through proper conversion method.
    Different types of result signals will be converted in different way..
    """

    def __init__(self, name, times, values):

        self.name = name
        self.times = times
        self.values = values

    def get_exportable(self, **kwargs):
        """
        This converts a specific result signal into an exportable signal
        :return:
        """

        return ExportableSignal(
            self.name,
            self.times,
            self.values,
            ** kwargs
        )


class ResultRunningSignal(ResultSignal):
    """
    Class for running type of signals (#cpu's, #job's, #nodes)
    """
    pass


class ResultProfiledSignal(ResultSignal):

    def __init__(self, name, times, values, binned_elapsed, binned_processes):

        super(ResultProfiledSignal, self).__init__(name, times, values)

        self.binned_elapsed = binned_elapsed
        self.binned_processes = binned_processes

    def get_exportable(self):
        """
        For the case of profiled result metrics, we export the ratios for each bin
        :return:
        """

        times_diff = np.diff(np.asarray(list(self.times)))
        _times = self.times[1:]
        _ratios = np.asarray(self.values[1:])/times_diff

        return ExportableSignal(self.name, _times, _ratios)


class ResultInstantRatesSignal(ResultSignal):

    def __init__(self, name, n_signal, b_signal):

        assert isinstance(n_signal, ResultProfiledSignal)
        assert isinstance(b_signal, ResultProfiledSignal)

        self.n_signal = n_signal
        self.b_signal = b_signal

        # form the signal..
        _times, _values = self.make_exportable_from_io_signal()

        super(ResultInstantRatesSignal, self).__init__(name, _times, _values)

    def make_exportable_from_io_signal(self):
        """
        For the case of profiled result metrics, we export the ratios for each bin
        :return:
        """

        n_t = self.n_signal.times
        # n_v = self.n_signal.values
        # n_e = self.n_signal.binned_elapsed
        # n_p = self.n_signal.binned_processes

        # b_t = b_signal.times
        b_v = self.b_signal.values
        b_e = self.b_signal.binned_elapsed
        # b_p = self.b_signal.binned_processes

        t_vals = np.asarray(n_t)
        b_vals = np.asarray(b_v)
        e_vals = np.asarray(b_e)
        # p_vals = np.asarray(b_p)

        rates = np.asarray([b / e if e else 0.0 for b, e in zip(b_vals, e_vals)])

        _values = rates
        _times = t_vals-t_vals[0]

        return _times, _values
