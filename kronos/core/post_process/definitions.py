# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import math
import datetime
import numpy as np


# ///////////////////////////////// DEFINITIONS /////////////////////////////////////////

labels_map = {
    'n_write': "#/sec",
    'kb_write': "Kb/sec",
    'n_read': "#/sec",
    'n_collective': "#/sec",
    'kb_read': "Kb/sec",
    'flops': "#/sec",
    'kb_collective': "Kb/sec",
    'kb_pairwise': "Kb/sec",
    'n_pairwise': "#/sec"
}

# (root name, job identifier) ["serial", "parallel"]
class_names_complete = [
    ('main/fc/inigroup', 'serial'),
    ('main/fc/inigroup', 'parallel'),
    ('main/fc/ensemble/cf', 'serial'),
    ('main/fc/ensemble/cf', 'parallel'),
    ('main/fc/ensemble/pf', 'serial'),
    ('main/fc/ensemble/pf', 'parallel'),
    ('main/fc/ensemble/logfiles', 'serial'),
    ('main/fc/lag', 'serial'),
]

class_colors = ["b", "r", "g", "m", "c"]


# ///////////////////////////////// UTILITIES /////////////////////////////////////////
def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970,1,1)).total_seconds()


def cumsum(input_list):
    return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]


def linspace(x0, x1, count):
    return [x0+(x1-x0)*i/float(count-1) for i in range(count)]


def running_series(jobs, times, t0_epoch_wl, job_class=None):

    bin_width = times[1] - times[0]

    running = np.zeros(len(times))
    found = 0

    for job in jobs:

        if job.is_in_class(job_class):
            
            found += 1
            first = int(math.ceil((job.t_start-t0_epoch_wl - times[0]) / bin_width))
            last = int(math.floor((job.t_end-t0_epoch_wl - times[0]) / bin_width))
            running[first:last] += 1

    return found, running
