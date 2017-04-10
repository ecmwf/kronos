# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos.core.kronos_tools.print_colour import print_colour


class ModelMeasure(object):
    """
    Simplest class to store measure information
    """
    def __init__(self, name, value, source_function):
        self.name = name
        self.value = value
        self.source_function = source_function


class Report(object):
    list_measures = []

    def __init__(self):
        pass

    @classmethod
    def add_measure(cls, measure):
        cls.list_measures.append(measure)

    @classmethod
    def print_report(cls):

        # calculate longest measure name
        print "\n____________ MODEL METRICS ______________"

        for m in cls.list_measures:

            # simple line if the metrics is a scalar
            if isinstance(m.value, int) or isinstance(m.value, float):
                print_colour("white", "\n{}: {} (calculated in {})\n".format(m.name, m.value, m.source_function), log_level="info")

            # print in multi-line is the metric is a dictionary:
            if isinstance(m.value, dict):

                print_colour("white", "\n{}: (calculated in {})".format(m.name, m.source_function))

                for k, v in m.value.iteritems():
                    print_colour("white", "{}: {}".format(k, v))



