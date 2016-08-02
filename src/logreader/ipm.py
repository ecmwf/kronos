from pylab import *

import os
import subprocess
import xml.etree.ElementTree as ET

from jobs import IngestedJob, ModelJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet


class IPMDataSet(IngestedDataSet):

    def model_jobs(self):
        """
        Model the Darshan jobs, given a list of injested jobs

        """
        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        # global_start_time = min((j.time_start for j in self.joblist))

        for job in self.joblist:
            yield job.model_job()


class IPMIngestedJob(IngestedJob):
    """
    N.B. Darshan may produce MULTIPLE output files for each of the actual HPC jobs (as it produces one per command
    that is run in the submit script).
    """
    # What fields are used by IPM (that are different to the defaults in IngestedJob)

    def model_job(self, first_start_time):
        """
        Return a ModelJob from the supplied information
        """
        return ModelJob()

    def aggregate(self, rhs):
        raise NotImplementedError


class IPMLogReader(LogReader):

    job_class = IPMIngestedJob
    dataset_class = IPMDataSet
    log_type_name = "IPM"
    file_pattern = "*.xml"

    # By default we end up with a whole load of darshan logfiles within a directory.
    label_method = "directory"

    def __init__(self, path, **kwargs):

        # TODO: Configure the darshan paths (need darshan-parser)
        print "IPM Log Reader"

        # Custom configuration:
        # self.parser_command = kwargs.pop('parser', 'darshan-parser')

        super(IPMLogReader, self).__init__(path, **kwargs)

    def read_log(self, filename, suggested_label):
        """
        Read a darshan log!
        """
        print "LOG: {}".format(filename)
        root = ET.parse(filename).getroot()
        return []

    def read_logs_generator(self):
        """
        In the same way as Darshan, IPM produces one log per command executed in the script. This results in multiple
        IPM files per job, which need to be aggregated. Each of the jobs will be sequential, so we combine them.
        """

        current_job = None

        for job in super(IPMLogReader, self).read_logs_generator():

            if current_job is None:
                current_job = job

            elif job.label == current_job.label:
                current_job.aggregate(job)

            else:
                yield current_job
                current_job = job

        # And when we are at the end of the list, yield the current job
        if current_job is not None:
            yield current_job

