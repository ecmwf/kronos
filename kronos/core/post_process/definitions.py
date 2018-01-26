# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import datetime

# ///////////////////////////////// UTILITIES /////////////////////////////////////////
def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970,1,1)).total_seconds()


def fig_name_from_class(class_name):
    return class_name.replace("/", "_").replace("*", "ANY")


def cumsum(input_list):
    return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]


def linspace(x0, x1, count):
    return [x0+(x1-x0)*i/float(count-1) for i in range(count)]


# ///////////////////////////////// DEFINITIONS /////////////////////////////////////////

labels_map = {
    'n_write': "[-]",
    'kb_write': "[KiB]",
    'n_read': "[-]",
    'n_collective': "[#]",
    'kb_read': "[KiB]",
    'flops': "[-]",
    'kb_collective': "[KiB]",
    'kb_pairwise': "[KiB]",
    'n_pairwise': "[-]",

    'jobs':  "[-]",
    'procs': "[-]",
    'nodes': "[-]",

    'write_rates': "[KiB/s]",
    'read_rates': "[KiB/s]",
}

# class_colors = [(0, 0, 1),
#                 (1, 0, 0),
#
#                 (0, 1, 0),
#                 (0, 1, 1),
#                 (1, 0, 1),
#
#                 (0.3, 0.3, 0),
#                 (0, 0.2, 0.5),
#                 (0.5, 0., 0.2),
#
#                 (0.2, 0.5, 0),
#                 (0.2, 0.5, 0.6),
#                 ]

class_colors = [(i, j, k) for i in linspace(0,1,3) for j in linspace(0,1,3) for k in linspace(0,1,3)]

plot_linestyle_sp = {
    "parallel": "-",
    "serial": "--",
}


def job_class_color(cl_in, class_list):
    class_list = list(class_list)
    cl_idx = class_list.index(cl_in)

    # return a rotated index (excluding 0, as black is reserved for all-classes plots)
    return class_colors[1:][cl_idx % len(class_colors)]