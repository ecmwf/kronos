# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import numpy as np


def mb(n_bytes):
    """ Mb from bytes """
    return n_bytes / 1024 / 1024


def freq_to_time(time, freqs, ampls, phases):
    """ from frequency to time domain """
    time_signal = np.zeros(len(time))
    for iF in np.arange(0, len(freqs)):
        time_signal = time_signal + \
                      ampls[iF] * np.sin(2 * np.pi * freqs[iF] * time + phases[iF])
    return time_signal


def sort_dict_list(d, sorted_keys_list):
    return [d[i] for i in sorted_keys_list]
