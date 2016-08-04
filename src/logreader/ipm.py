from pylab import *

import xml.etree.ElementTree as ET

from jobs import IngestedJob, ModelJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet
from time_signal import TimeSignal


class IPMTaskInfo(object):
    """
    Store the information aggregated in an IPM task.
    """
    def __init__(self):

        self.mpi_pairwise_count_send = 0
        self.mpi_pairwise_bytes_send = 0
        self.mpi_pairwise_count_recv = 0
        self.mpi_pairwise_bytes_recv = 0

        self.mpi_collective_count = 0
        self.mpi_collective_bytes = 0

        self.open_count = 0
        self.read_count = 0
        self.write_count = 0
        self.bytes_read = 0
        self.bytes_written = 0

    def __unicode__(self):
        return "IPMTaskInfo({} MPI events, {} MPI bytes, {} IO events, {} IO bytes)".format(
            self.mpi_pairwise_count_send + self.mpi_pairwise_count_recv + self.mpi_collective_count,
            self.mpi_pairwise_bytes_send + self.mpi_pairwise_bytes_recv + self.mpi_collective_bytes,
            self.open_count + self.read_count + self.write_count,
            self.bytes_read + self.bytes_written
        )

    def __str__(self):
        return unicode(self).encode('utf-8')


class IPMIngestedJob(IngestedJob):
    """
    N.B. Darshan may produce MULTIPLE output files for each of the actual HPC jobs (as it produces one per command
    that is run in the submit script).
    """
    # What fields are used by IPM (that are different to the defaults in IngestedJob)

    tasks = []

    def model_job(self):
        """
        Return a ModelJob from the supplied information
        """
        return ModelJob(
            label=self.label,
            time_series=self.model_time_series(),
            time_start=-1,
            ncpus=-1,
            nnodes=-1
        )

    def aggregate(self, rhs):

        # We want to include all of the tasks that have been IPM'd
        self.tasks += rhs.tasks

    def model_time_series(self):

        total_mpi_pairwise_count_send = 0
        total_mpi_pairwise_bytes_send = 0
        total_mpi_pairwise_count_recv = 0
        total_mpi_pairwise_bytes_recv = 0

        total_mpi_collective_count = 0
        total_mpi_collective_bytes = 0

        total_read_count = 0
        total_write_count = 0
        total_bytes_read = 0
        total_bytes_written = 0

        for task in self.tasks:
            total_mpi_pairwise_count_send += task.mpi_pairwise_count_send
            total_mpi_pairwise_bytes_send += task.mpi_pairwise_bytes_send
            total_mpi_pairwise_count_recv += task.mpi_pairwise_count_recv
            total_mpi_pairwise_bytes_recv += task.mpi_pairwise_bytes_recv

            total_mpi_collective_count += task.mpi_collective_count
            total_mpi_collective_bytes += task.mpi_collective_bytes

            total_read_count += task.read_count
            total_write_count += task.write_count
            total_bytes_read += task.bytes_read
            total_bytes_written += task.bytes_written

        # n.b. only using the pairwise send data. Recv should be largely a duplicate, but slightly smaller
        #      as MPI_Sendrecv is only being counted under send for now. If we used both send and recv data
        #      from _all_ tasks we would double count the transfers.

        return {
            'n_collective': TimeSignal.from_values('n_collective', [0.0], [total_mpi_collective_count]),
            'kb_collective': TimeSignal.from_values('kb_collective', [0.0],
                                                    [float(total_mpi_collective_bytes) / 1024.0]),

            'n_pairwise': TimeSignal.from_values('n_pairwise', [0.0], [total_mpi_pairwise_count_send]),
            'kb_pairwise': TimeSignal.from_values('kb_pairwise', [0.0],
                                                  [float(total_mpi_pairwise_bytes_send) / 1024.0]),

            'kb_read': TimeSignal.from_values('kb_read', [0.0], [float(total_bytes_read) / 1024.0]),
            'kb_write': TimeSignal.from_values('kb_write', [0.0], [float(total_bytes_written) / 1024.0]),

            # TODO: Make use of read/write counts
        }



class IPMLogReader(LogReader):

    job_class = IPMIngestedJob
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
        "MPI_Waitall": None,
        "MPI_Waitany": None,
        "MPI_Waitsome": None,
        "MPI_Buffer_attach": None,
        "MPI_Buffer_detach": None,
        "MPI_Barrier": None,
        "MPI_Probe": None,
        "MPI_Iprobe": None,
        "MPI_Send_init": None,
        "MPI_Ssend_init": None,
        "MPI_Rsend_init": None,
        "MPI_Bsend_init": None,
        "MPI_Recv_init": None,
        "MPI_Comm_group": None,
        "MPI_Comm_compare": None,
        "MPI_Comm_dup": None,
        "MPI_Comm_create": None,
        "MPI_Comm_split": None,
        "MPI_Comm_free": None,
        "MPI_Test": None,
        "MPI_Testany": None,
        "MPI_Testall": None,
        "MPI_Testsome": None,
        "MPI_Start": None,
        "MPI_Startall": None,

        "MPI_Send": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Bsend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Rsend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Ssend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Isend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Issend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Irsend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Ibsend": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},

        # TODO: How should we account for _sendrecv?
        "MPI_Sendrecv": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},
        "MPI_Sendrecv_replace": {"count": 'mpi_pairwise_count_send', "bytes": 'mpi_pairwise_bytes_send'},

        "MPI_Recv": {"count": 'mpi_pairwise_count_recv', "bytes": 'mpi_pairwise_bytes_recv'},
        "MPI_Irecv": {"count": 'mpi_pairwise_count_recv', "bytes": 'mpi_pairwise_bytes_recv'},

        # Todo: Do we need to scale all-all and all-one collecvise seperately
        "MPI_Bcast": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Gather": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Gatherv": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Allgather": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Allgatherv": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Allreduce": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Alltoallv": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Alltoall": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Reduce": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Reduce_scatter": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Scatter": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Scatterv": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},
        "MPI_Scan": {"count": 'mpi_collective_count', "bytes": 'mpi_collective_bytes'},

        # And now for the POSIXIO ones
        "fopen": {"count": 'open_count'},
        "fdopen": {"count": 'open_count'},
        "freopen": {"count": 'open_count'},
        "fclose": None,
        "fflush": None,
        "fread": {"count": 'read_count', "bytes": "bytes_read"},
        "fwrite": {"count": "write_count", "bytes": "bytes_written"},
        "fseek": None,
        "ftell": None,
        "rewind": None,
        "fgetpos": None,
        "fsetpos": None,
        "fgetc": {"count": 'read_count', "bytes": "bytes_read"},
        "getc": {"count": 'read_count', "bytes": "bytes_read"},
        "ungetc": None,
        "read": {"count": 'read_count', "bytes": "bytes_read"},
        "write": {"count": 'read_count', "bytes": "bytes_read"},
        "open": {"count": "open_count"},
        "open64": {"count": "open_count"},
        "creat": None,
        "close": None,
        "truncate": None,
        "ftruncate": None,
        "truncate64": None,
        "ftruncate64": None,
        "lseek": None,
        "lseek64": None
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
                name = entry.get('name')

                if entry.get('name') not in self.func_mapping:
                    # TODO: A proper exception here...
                    raise Exception("Oh no... {} {}".format(module, entry.get('name')))

    def parse_task(self, task, ntasks):

        assert int(task.get('mpi_size')) == ntasks
        assert int(task.find('job').get('ntasks')) == ntasks

        regions_container = task.find('regions')
        nregions = int(regions_container.get('n'))

        regions = regions_container.findall('region')
        assert len(regions) == nregions

        task = IPMTaskInfo()

        for region in regions:

            funcs = region.findall('func')
            for func in funcs:

                # n.b. We have checked all available function names in parse_calltable above. If one is
                #      not in the lookup, this is a bug.
                name = func.get('name')
                assert name in self.func_mapping

                mapping = self.func_mapping.get(name, None)
                if mapping is not None:
                    for key, task_attr in mapping.items():

                        val = float(func.get(key))
                        assert val.is_integer()

                        # Keep track of the counters
                        assert hasattr(task, task_attr)
                        setattr(task, task_attr, getattr(task, task_attr) + int(val))

        return task

    def read_log(self, filename, suggested_label):
        """
        Read a darshan log!
        """
        root = ET.parse(filename).getroot()

        # Look at the available opts
        assert root.tag == "ipm_job_profile"

        self.parse_calltable(root.find('calltable'))

        tasks = root.findall('task')
        ntasks = len(tasks)
        assert ntasks > 0
        task_info = [self.parse_task(task, ntasks) for task in tasks]

        return [IPMIngestedJob(
            tasks=task_info,
            label=suggested_label,
            filename=filename
        )]

    def read_logs(self):
        """
        In the same way as Darshan, IPM produces one log per command executed in the script. This results in multiple
        IPM files per job, which need to be aggregated. Each of the jobs will be sequential, so we combine them.
        """

        current_job = None

        for job in super(IPMLogReader, self).read_logs():

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


class IPMDataSet(IngestedDataSet):

    log_reader_class = IPMLogReader
