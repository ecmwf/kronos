from schema_description import SchemaDescription
from workload_data import WorkloadData
import time_signal
from jobs import ModelJob

import strict_rfc3339

from datetime import datetime
import json
import jsonschema
import os


class ProfileFormat(object):
    """
    A standardised format for profiling information.
    """
    kpf_version = 1
    kpf_magic = "KRONOS-KPF-MAGIC"

    def __init__(self, model_jobs=None, json_jobs=None, created=None, uid=None, workload_tag="unknown"):

        # We either initialise from model jobs, or from processed json data
        assert (model_jobs is not None) != (json_jobs is not None)
        if model_jobs:
            self.profiled_jobs = [self.parse_model_job(m) for m in model_jobs]
        else:
            self.profiled_jobs = json_jobs

        self.created = created
        self.uid = uid
        self.workload_tag = workload_tag

    def __unicode__(self):
        return "KronosProfileFormat(njobs={})".format(len(self.profiled_jobs))

    def __str__(self):
        return unicode(self).encode('utf-8')

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

        # Append any time series data that is present
        time_series = {}
        for name, values in model_job.timesignals.iteritems():
            assert name in time_signal.signal_types
            if values is not None:
                time_series[name] = {
                    'times': values.xvalues,
                    'values': values.yvalues
                }

        # The time series data is only included if it is present.
        if time_series:
            job['time_series'] = time_series
        return job

    @staticmethod
    def from_file(f):
        """
        Given a KPF file, load it and make the data appropriately available
        """
        data = json.load(f)
        ProfileFormat.validate_json(data)

        return ProfileFormat(
            json_jobs=data['profiled_jobs'],
            created=datetime.fromtimestamp(strict_rfc3339.rfc3339_to_timestamp(data['created'])),
            uid=data['uid'],
            workload_tag=data['workload_tag']
        )

    @staticmethod
    def from_filename(filename):
        with open(filename, 'r') as f:
            return ProfileFormat.from_file(f)

    def write(self, f):
        """
        Exports the profile as a (standardised) kpf file.
        TODO: Documentation of the format
        """
        output_dict = {
            "version": self.kpf_version,
            "tag": self.kpf_magic,
            "created": strict_rfc3339.now_to_rfc3339_utcoffset(),
            "uid": os.getuid(),
            "profiled_jobs": self.profiled_jobs,
            "workload_tag": self.workload_tag
        }

        self.validate_json(output_dict)

        json.dump(output_dict, f)

    def write_filename(self, filename):
        with open(filename, 'w') as f:
            self.write(f)

    def model_jobs(self):
        """
        Re-animate ModelJobs from the profiled data (in json form)
        """
        for job in self.profiled_jobs:
            yield ModelJob(
                timesignals={n: time_signal.TimeSignal(n, xvalues=t['times'], yvalues=t['values'])
                             for n, t in job.get('time_series', {}).iteritems()},
                **{k: v for k, v in job.iteritems() if k in ['time_queued',
                                                             'time_start',
                                                             'duration',
                                                             'ncpus',
                                                             'nnodes',
                                                             'label'] and v is not None}
            )

    def workload(self):
        """
        Obtain a workload for further use in modelling (attach the appropriate tag)
        """
        return WorkloadData(jobs=self.model_jobs(), tag=self.workload_tag)

    @classmethod
    def schema(cls):
        """
        Obtain the json schema for the ProfileFormat
        """
        with open(os.path.join(os.path.dirname(__file__), "profile_schema.json"), 'r') as fschema:
            str_schema = fschema.read() % {
                "kronos-version": cls.kpf_version,
                "kronos-magic": cls.kpf_magic
            }
            return json.loads(str_schema)

    @classmethod
    def validate_json(cls, js):
        """
        Do validation of a dictionary that has been loaded from (or will be written to) a JSON
        """
        jsonschema.validate(js, cls.schema(), format_checker=jsonschema.FormatChecker())

    @classmethod
    def describe(cls):
        """
        Output a description of the JSON in human-readable format
        """
        print SchemaDescription.from_schema(cls.schema())

