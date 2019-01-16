# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np


def digitize_xyvalues(xvalues, yvalues, nbins=None, key="sum"):
    """
    On-the-fly return digitized time series (rather than using
    """

    if nbins is None:
        return xvalues, yvalues

    # some checks..
    assert len(xvalues) == len(yvalues)
    nbins = int(nbins)
    xvalues = np.asarray(xvalues)
    yvalues = np.asarray(yvalues)

    # Determine the bin boundaries
    xedge_bins = np.linspace(min(xvalues), max(xvalues) + 1.0e-6, nbins + 1)

    # Return xvalues as the midpoints of the bins
    bins_delta = xedge_bins[1] - xedge_bins[0]
    xvalues_bin = xedge_bins[1:] - (0.5 * bins_delta)

    # Split the data up amongst the bins
    # n.b. Returned indices will be >= 1, as 0 means to the left of the left-most edge.
    bin_indices = np.digitize(xvalues, xedge_bins)

    yvalues_bin = np.zeros(nbins)
    for i in range(nbins):
        if any(yvalues[bin_indices == i+1]):
            if key == 'mean':
                val = yvalues[bin_indices == i+1].mean()
            elif key == 'sum':
                val = yvalues[bin_indices == i+1].sum()
            else:
                raise ValueError("Digitization key value not recognised: {}".format(key))

            yvalues_bin[i] = val

    return xvalues_bin, yvalues_bin

