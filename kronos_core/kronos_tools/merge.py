# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.



def max_not_none(*args):
    """
    If all of the elements are None, return None, otherwise return the largest non-None element
    """
    if any(x is not None for x in args):
        return max(x for x in args if x is not None)
    return None


def min_not_none(*args):
    """
    If all of the elements are None, return None, otherwise return the smallest non-None element
    """
    if any(x is not None for x in args):
        return min(x for x in args if x is not None)
    return None
