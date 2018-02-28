# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


def add_value_to_sublist(_list, idx1, idx2, val):
    _list[idx1:idx2] = [_list[i]+val for i in range(idx1, idx2)]
    return _list
