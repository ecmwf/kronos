import sys
#from collections import OrderedDict

#from jobs import ModelJob
#from kronos_tools import print_colour
#from time_signal import TimeSignal
#from workload_data import WorkloadData
from schema_description import SchemaDescription
import time_signal

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

    def __init__(self, model_jobs=None, json_jobs=None, created=None, uid=None):

        # We either initialise from model jobs, or from processed json data
        assert (model_jobs is not None) != (json_jobs is not None)
        if model_jobs:
            self.profiled_jobs = [self.parse_model_job(m) for m in model_jobs]
        else:
            self.profiled_jobs = json_jobs

        self.created = created
        self.uid = uid

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
            if (j1['time_end'] != j2['time_end'] or
                    j1['time_start'] != j2['time_start'] or
                    j1['duration'] != j2['duration'] or
                    j1['ncpus'] != j2['ncpus'] or
                    j1['nnodes'] != j2['nnodes'] or
                    j1['label'] != j2['label']):
                return False

            if (j1.get('time_series', None) is not None) != (j2.get('time_series', None) is not None):
                return False

            if j1.get('time_series', None) is not None:
                for name, values in j1['time_series'].iteritems():
                    if name not in j2['time_series']:
                        return False
                    if (values.get('times', None) != j2['time_series'][name].get('times', None) or
                            values.get('values', None) != j2['time_series'][name].get('values', None)):
                        return False

        return True

    @staticmethod
    def parse_model_job(model_job):
        job = {
            "time_end": model_job.time_queued,
            "time_start": model_job.time_start,
            "duration": model_job.duration,
            "ncpus": model_job.ncpus,
            "nnodes": model_job.nnodes,
            "label": model_job.label,
        }

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
            uid=data['uid']
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
            "profiled_jobs": self.profiled_jobs
        }

        self.validate_json(output_dict)

        json.dump(output_dict, f)

    def write_filename(self, filename):
        with open(filename, 'w') as f:
            self.write(f)

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


try:
    import cPickle as pickle
except:
    import pickle
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

if __name__ == "__main__":

    ProfileFormat.describe()

    with open(sys.argv[1], 'r') as f:
        ds = pickle.load(f)

    model_jobs = ds.model_jobs()

    with open('output.kpf', 'w') as f:

        pf = ProfileFormat(model_jobs)
        print pf
        pf.write(f)

    with open('output.kpf', 'r') as f:
        pf2 = ProfileFormat.from_file(f)
        print pf2

    print pf == pf2
