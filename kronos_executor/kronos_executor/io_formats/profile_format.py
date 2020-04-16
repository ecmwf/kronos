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


class ProfileFormat(JSONIoFormat):
    """
    A standardised format for profiling information.
    """
    format_version = 1
    format_magic = "KRONOS-KPROFILE-MAGIC"
    schema_json = os.path.join(os.path.dirname(__file__), "profile_schema.json")

    def __init__(self, model_jobs=None, json_jobs=None, created=None, uid=None, workload_tag="unknown"):

        super(ProfileFormat, self).__init__(created=created, uid=uid)

        # We either initialise from model jobs, or from processed json data
        assert (model_jobs is not None) != (json_jobs is not None)

        if model_jobs:
            self.profiled_jobs = [self.parse_model_job(m) for m in model_jobs]
        else:
            self.profiled_jobs = json_jobs

        self.workload_tag = workload_tag

    def __eq__(self, other):
        """
        Are two profiles the same?
        """
        # We don't test the UID/created timestamp. Not interesting. Only care about the data.

        if len(self.profiled_jobs) != len(other.profiled_jobs):
            return False

        for j1, j2 in zip(self.profiled_jobs, other.profiled_jobs):
            if j1 != j2:
                return False

        return True

    def __ne__(self, other):
        return not (self == other)

    @staticmethod
    def parse_model_job(model_job):
        job = {}

        if model_job.time_queued is not None:
            job['time_queued'] = model_job.time_queued
        if model_job.time_start is not None:
            job['time_start'] = model_job.time_start
        if model_job.duration is not None:
            job['duration'] = model_job.duration
        if model_job.ncpus is not None:
            job['ncpus'] = model_job.ncpus
        if model_job.nnodes is not None:
            job['nnodes'] = model_job.nnodes
        if model_job.label is not None:
            job['label'] = model_job.label

        # TODO: think about a better place for the job name..
        if model_job.job_name is not None:
            job['job_name'] = model_job.job_name

        # Append any time series data that is present
        time_series = {}
        for name, values in model_job.timesignals.items():

            if values is not None:
                time_series[name] = {
                    'times': list(values.xvalues),
                    'values': list(values.yvalues),
                    'priority': values.priority
                }

        # The time series data is only included if it is present.
        if time_series:
            job['time_series'] = time_series

        return job

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        return cls(
            json_jobs=data['profiled_jobs'],
            created=datetime.fromtimestamp(strict_rfc3339.rfc3339_to_timestamp(data['created'])),
            uid=data['uid'],
            workload_tag=data['workload_tag']
        )

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """
        output_dict = super(ProfileFormat, self).output_dict()
        output_dict.update({
            "profiled_jobs": self.profiled_jobs,
            "workload_tag": self.workload_tag
        })
        return output_dict
