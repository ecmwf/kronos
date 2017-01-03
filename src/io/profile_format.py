import sys
#from collections import OrderedDict

#from jobs import ModelJob
#from kronos_tools import print_colour
#from time_signal import TimeSignal
#from workload_data import WorkloadData
from schema_description import SchemaDescription
import time_signal

import strict_rfc3339

import json
import jsonschema
import os


class ProfileFormat(object):
    """
    A standardised format for profiling information.
    """
    kpf_version = 1
    kpf_magic = "KRONOS-KPF-MAGIC"

    def __init__(self, model_jobs=[]):
        self.profiled_jobs = [self.parse_model_job(m) for m in model_jobs]

    def __unicode__(self):
        return "KPF(njobs={})".format(len(self.profiled_jobs))

    def __str__(self):
        return unicode(self).encode('utf-8')

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

        pf = ProfileFormat()
        pf.profiled_jobs = data['profiled_jobs']
        return pf

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
    print model_jobs


    with open('output.kpf', 'w') as f:

        pf = ProfileFormat(model_jobs)
        print pf
        pf.write(f)

    with open('output.kpf', 'r') as f:
        pf = ProfileFormat.from_file(f)
        print pf

#class KPFFileHandler(object):
#    """
#    Class that encapsulates some of the operations for a kpf file
#    """
#    def __init__(self):
#        pass
#
#    @staticmethod
#    def save_kpf(workloads, kpf_filename=None):
#        """
#        Export a kpf file
#        :param workloads: list of workloads
#        :param kpf_filename:
#        :return:
#        """
#
#        # if the workloads is only one workload, translate it to a list
#        if isinstance(workloads, WorkloadData):
#            workloads = [workloads]
#        else:
#            assert all(isinstance(wl, WorkloadData) for wl in workloads)
#
#        print_colour.print_colour('white', 'exporting kpf file: {}'.format(kpf_filename))
#
#        if kpf_filename:
#
#            kpf_data = []
#
#            if not kpf_filename.endswith('.kpf'):
#                print("extension .ksp will be appended")
#                kpf_filename += '.kpf'
#
#            for wl in workloads:
#
#                # collect data from jobs in the dataset
#                wl_jobs_json = []
#                for job in wl.jobs:
#
#                    job_entry_data = OrderedDict([
#                                                ('job_name', job.job_name),
#                                                ('user_name', job.user_name),
#                                                ('cmd_str', job.cmd_str),
#                                                ('queue_name', job.queue_name),
#                                                ('time_queued', job.time_queued),
#                                                ('time_start', job.time_start),
#                                                ('duration', job.duration),
#                                                ('ncpus', job.ncpus),
#                                                ('nnodes', job.nnodes),
#                                                ('stdout', job.stdout),
#                                                ('label', job.label),
#                                                ])
#
#                    # # automatically creates a dictionary with all the class attributes..
#                    # # excluding callable functions and properties
#                    # job_entry_data = {k: v for k, v in ModelJob.__dict__.items()
#                    #                   if not k.startswith('__') and not callable(v) and not isinstance(v, property)}
#
#                    # append timeseries signals
#                    timesignals_dict = {}
#                    for tsk, tsv in job.timesignals.items():
#                        if tsv:
#                            timesignals_dict[tsk] = OrderedDict([
#                                ('xvalues', list(tsv.xvalues)),
#                                ('yvalues', list(tsv.yvalues)),
#                            ])
#
#                    job_entry_data['timesignals'] = timesignals_dict
#
#                    # append this job to the dataset job list
#                    wl_jobs_json.append(job_entry_data)
#
#                kpf_workload_entry = OrderedDict([
#                                                ('tag', wl.tag),
#                                                ('jobs', wl_jobs_json)
#                                                ])
#
#                # for each job write the relevant data
#                kpf_data.append(kpf_workload_entry)
#
#            # export kpf file
#            with open(kpf_filename, 'w') as f:
#                json.dump(kpf_data, f, indent=4, separators=(',', ': '))
#        else:
#            print "filename not specified!"
#            sys.exit(-1)
#
#    @staticmethod
#    def load_kpf(kpf_filename):
#        """
#        Load a kpf file and returns a workload
#        :param kpf_filename:
#        :return: jobs
#        """
#
#        # list of job sets to return
#        workloads = []
#
#        with open(kpf_filename, 'r') as f:
#            workloads_json = json.load(f)
#
#        for wl_json in workloads_json:
#            jobs = wl_json['jobs']
#            model_jobs = []
#
#            for job in jobs:
#
#                # create dictionary of timesignals
#                ts_dict = {}
#                for tsk, tsv in job['timesignals'].items():
#                    ts_dict[tsk] = TimeSignal(tsk).from_values(tsk,
#                                                               tsv['xvalues'],
#                                                               tsv['yvalues'],
#                                                               )
#
#                model_jobs.append(
#                                    ModelJob(
#                                            job_name=job['job_name'],
#                                            user_name=job['user_name'],
#                                            cmd_str=job['cmd_str'],
#                                            queue_name=job['queue_name'],
#                                            time_queued=job['time_queued'],
#                                            time_start=job['time_start'],
#                                            duration=job['duration'],
#                                            ncpus=job['ncpus'],
#                                            nnodes=job['nnodes'],
#                                            stdout=job['stdout'],
#                                            label=job['label'],
#                                            timesignals=ts_dict,
#                                            )
#                                 )
#
#            workloads.append(WorkloadData(jobs=model_jobs, tag=wl_json['tag']))
#
#        # return the workload
#        return workloads
