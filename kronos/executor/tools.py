# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


def mean_of_list(vals):
    """
    avg value of a list
    :param vals:
    :return:
    """
    return sum(vals)/float(len(vals))


def std_of_list(vals):
    """
    std of a list
    :param vals:
    :return:
    """
    mean_val = mean_of_list(vals)
    return (sum([(v-mean_val)**2 for v in vals])/float(len(vals)))**0.5
