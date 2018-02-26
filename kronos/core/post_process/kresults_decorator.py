# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


class KResultsDecorator(object):
    """
    Job decorator data
    """

    # Required fields (and relative mapped attribute names)
    required_info = [
        ("workload_name", "label"),
        ("job_name", "name"),
    ]

    def __init__(self, **kwargs):

        # for each of the required fields check the arguments
        for req_attr, attr_name in self.required_info:

            # if found, attach the mapped argument name
            if req_attr in kwargs.keys():
                setattr(self, attr_name, kwargs[req_attr])
            else:
                raise AttributeError("Attribute {} of {} not set but required!".format(attr_name, self.__class__.__name__))
