# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
from datetime import datetime

import strict_rfc3339

from kronos_executor.io_formats.json_io_format import JSONIoFormat


class ScheduleFormat(JSONIoFormat):
    """
    A standardised format for time_schedule information.
    """
    format_version = 3
    format_magic = "KRONOS-KSCHEDULE-MAGIC"
    schema_json = os.path.join(os.path.dirname(__file__), "schedule_schema.json")

    def __init__(self,
                 sa_data_json=None,
                 sa_data=None,
                 created=None,
                 uid=None,
                 n_bins=None,
                 trunc_pc=None,
                 prologue=None,
                 epilogue=None,
                 ):

        # We either initialise from synthetic apps objects, or from synthetic apps json data
        assert (sa_data is not None) != (sa_data_json is not None)
        if sa_data:
            sorted_apps = sorted(sa_data, key=lambda a: a.time_start)
            sa_data_json = [app.export('',
                                       job_entry_only=True,
                                       trunc_pc=trunc_pc,
                                       n_bins=n_bins) for i, app in enumerate(sorted_apps)]

        # initialize internals
        self.synapp_data = sa_data_json

        # prologue/epilogue info in the kschedule
        self.prologue = prologue
        self.epilogue = epilogue

        super(ScheduleFormat, self).__init__(created=created, uid=uid)

    def __eq__(self, other):
        """
        Are two schedules the same?
        """
        # We don't test the UID/created timestamp. Not interesting. Only care about the data.

        if len(self.synapp_data) != len(other.sa_data):
            return False

        for j1, j2 in zip(self.synapp_data, other.sa_data):
            if j1 != j2:
                return False

        return True

    def __ne__(self, other):
        return not (self == other)

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        return cls(
            sa_data_json=data.get('jobs'),
            created=datetime.fromtimestamp(strict_rfc3339.rfc3339_to_timestamp(data['created'])),
            uid=data.get('uid'),
            prologue=data.get('prologue'),
            epilogue=data.get('epilogue')
        )

    @classmethod
    def from_synthetic_workload(cls, synth_workload, n_bins, trunc_pc=None):
        """
        Creates a kschedule handler from synthetic workload data
        :param synth_workload:
        :param n_bins:
        :param trunc_pc:
        :return:
        """

        return cls(
            sa_data=synth_workload.app_list,
            n_bins=n_bins,
            trunc_pc=trunc_pc
        )

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """
        output_dict = super(ScheduleFormat, self).output_dict()
        output_dict.update({
            "jobs": self.synapp_data
        })

        # add prologue/epilogue if they exist
        if self.prologue:
            output_dict.update({"prologue": self.prologue})

        if self.epilogue:
            output_dict.update({"epilogue": self.epilogue})

        return output_dict

    def set_scaling_factors(self, scaling_factors):
        """
        Set the scaling factors
        :param scaling_factors:
        :return:
        """
        self.scaling_factors = scaling_factors
