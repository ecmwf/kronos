# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import datetime


def add_value_to_sublist(_list, idx1, idx2, val):
    _list[idx1:idx2] = [_list[i]+val for i in range(idx1, idx2)]
    return _list


def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970, 1, 1)).total_seconds()


def fig_name_from_class(class_name):
    return class_name.replace("/", "_").replace("*", "ANY")


def cumsum(input_list):
    return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]


def linspace(x0, x1, count):
    return [x0+(x1-x0)*i/float(count-1) for i in range(count)]

