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
    ('main/fc/inigroup/*', 'serial'),
    ('main/fc/inigroup/*', 'parallel'),

    ('main/fc/ensemble/cf/control/legA/getiniLeg/*', 'parallel'),
    ('main/fc/ensemble/cf/control/legA/chunk0/*', 'parallel'),
    ('main/fc/ensemble/cf/*', 'serial'),

    ('main/fc/ensemble/pf/*', 'serial'),
    ('main/fc/ensemble/pf/*/legA/getiniLeg/*', 'parallel'),
    ('main/fc/ensemble/pf/*/legA/chunk0/modeleps_nemo/*', 'parallel'),

    ('main/fc/ensemble/logfiles/*', 'serial'),
    ('main/fc/lag/*', 'serial'),
]

class_colors = [(0, 0, 1),
                (1, 0, 0),

                (0, 1, 0),
                (0, 1, 1),
                (1, 0, 1),

                (0.3, 0.3, 0),
                (0, 0.2, 0.5),
                (0.5, 0., 0.2),

                (0.2, 0.5, 0),
                (0.2, 0.5, 0.6),
                ]

plot_linestyle_sp = {
    "parallel": "-",
    "serial": "--",
}


# ///////////////////////////////// UTILITIES /////////////////////////////////////////
def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970,1,1)).total_seconds()


def job_class_string(cl):
    return cl[0]+"/"+cl[1]


def job_class_color(cl_in):
    cl_set_idx = [cc for cc, cl_n in enumerate(set([n[0] for n in class_names_complete])) if cl_n == cl_in[0]]
    assert len(cl_set_idx) == 1
    return class_colors[cl_set_idx[0] % len(class_colors)]


def fig_name_from_class(class_name):
    return class_name.replace("/", "_").replace("*", "ANY")


def cumsum(input_list):
    return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]


def linspace(x0, x1, count):
    return [x0+(x1-x0)*i/float(count-1) for i in range(count)]


def running_series(jobs, times, t0_epoch_wl, n_procs_node=None, job_class=None):

    bin_width = times[1] - times[0]

    # logs number of concurrently running jobs, processes
    if n_procs_node:
        running_jpn = np.zeros((len(times), 3))
    else:
        running_jpn = np.zeros((len(times), 2))

    found = 0
    for job in jobs:

        if job.is_in_class(job_class):
            
            found += 1
            first = int(math.ceil((job.t_start-t0_epoch_wl - times[0]) / bin_width))
            last = int(math.floor((job.t_end-t0_epoch_wl - times[0]) / bin_width))

            # last index should always be >= first+1
            last = last if last > first else first+1

            # #jobs
            running_jpn[first:last, 0] += 1

            # #cpus
            running_jpn[first:last, 1] += job.n_cpu

            if n_procs_node:

                # #nodes
                n_nodes = job.n_cpu/int(n_procs_node) if not job.n_cpu%int(n_procs_node) else job.n_cpu/int(n_procs_node)+1
                running_jpn[first:last, 2] += n_nodes

    return found, running_jpn
