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

    func_mapping = {
        "MPI_Init": None,
        "MPI_Init_thread": None,
        "MPI_Finalize": None,
        "MPI_Comm_rank": None,
        "MPI_Comm_size": None,
        "MPI_Wait": None,
        "MPI_Waitany": None,
        "MPI_Buffer_attach": None,
        "MPI_Buffer_detach": None,
        "MPI_Barrier": None,
        "MPI_Comm_group": None,
        "MPI_Comm_compare": None,
        "MPI_Comm_dup": None,
        "MPI_Comm_create": None,

        "MPI_Send": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Bsend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Rsend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Ssend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Isend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Issend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Irsend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),
        "MPI_Ibsend": ('mpi_pairwise_count_send', 'mpi_pairwise_bytes_send'),

        "MPI_Recv": ('mpi_pairwise_count_recv', 'mpi_pairwise_bytes_recv'),
        "MPI_Irecv": ('mpi_pairwise_count_recv', 'mpi_pairwise_bytes_recv'),

        "MPI_Bcast": ('mpi_collective_count', 'mpi_collective_bytes'),
        "MPI_Gatherv": ('mpi_collective_count', 'mpi_collective_bytes'),
        "MPI_Allgatherv": ('mpi_collective_count', 'mpi_collective_bytes'),
        "MPI_Allreduce": ('mpi_collective_count', 'mpi_collective_bytes'),
        "MPI_Alltoallv": ('mpi_collective_count', 'mpi_collective_bytes')
    }

    def __init__(self, path, **kwargs):

        # TODO: Configure the darshan paths (need darshan-parser)
        print "IPM Log Reader"

        # Custom configuration:
        # self.parser_command = kwargs.pop('parser', 'darshan-parser')

        super(IPMLogReader, self).__init__(path, **kwargs)

    def parse_calltable(self, calltable):

        assert calltable is not None

        sections = calltable.findall('section')
        for section in sections:

            module = section.get('module')
            entries = section.findall('entry')
            assert len(entries) == int(section.get('nentries'))
            for entry in entries:
                print "{} : {}".format(module, entry.get('name'))

                if entry.get('name') not in self.func_mapping:
                    raise Exception("Oh no... {} {}".format(module, entry.get('name')))


    def parse_tasks(self, tasks):

        assert len(tasks) > 0

        for task in tasks:

            assert int(task.get('mpi_size')) == len(tasks)
            assert int(task.find('job').get('ntasks')) == len(tasks)

            regions_container = task.find('regions')
            nregions = int(regions_container.get('n'))

            regions = regions_container.findall('region')
            assert len(regions) == nregions

            for region in regions:

                funcs = region.findall('func')
                for func in funcs:
                    print "F: {} {} {}".format(func.get('name'), func.get('count'), func.get('bytes'))

    def read_log(self, filename, suggested_label):
        """
        Read a darshan log!
        """
        root = ET.parse(filename).getroot()

        # Look at the available opts
        assert root.tag == "ipm_job_profile"

        self.parse_calltable(root.find('calltable'))

        self.parse_tasks(root.findall('task'))

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

