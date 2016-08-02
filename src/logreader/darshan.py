from pylab import *

import os
import subprocess

from jobs import IngestedJob, ModelJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet
from time_signal import TimeSignal
from tools.print_colour import print_colour


class DarshanLogReaderError(Exception):
    pass


class DarshanDataSet(IngestedDataSet):

    def model_jobs(self):
        """
        Model the Darshan jobs, given a list of injested jobs

        """
        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        global_start_time = min((j.time_start for j in self.joblist))

        for job in self.joblist:
            yield job.model_job(global_start_time)

class DarshanIngestedJobFile(object):
    """
    An object to represent the file information available in a Darshan job

    This is a separate class, rather than being inside DarshanIngestedJob, as we need to pickle data to send it
    between processes, and pickling fails for nested classes.
    """
    def __init__(self, name):
        self.name = name

        self.bytes_read = 0
        self.bytes_written = 0
        self.open_count = 0
        self.write_count = 0
        self.read_count = 0

        self.read_time = None
        self.write_time = None

    def __unicode__(self):
        return "DarshanFile({} reads, {} bytes, {} writes, {} bytes)".format(self.read_count, self.bytes_read, self.write_count, self.bytes_written)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def aggregate(self, other):
        """
        Combine the data contents of two file objects
        """
        assert self.name == other.name

        self.bytes_read += other.bytes_read
        self.bytes_written += other.bytes_written
        self.open_count += other.open_count
        self.write_count += other.write_count
        self.read_count += other.read_count

        if self.read_time is not None:
            if other.read_time is not None:
                self.read_time = min(self.read_time, other.read_time)
        else:
            self.read_time = other.read_time

        if self.write_time is not None:
            if other.write_time is not None:
                self.write_time = min(self.write_time, other.write_time)
        else:
            self.write_time = other.write_time


class DarshanIngestedJob(IngestedJob):
    """
    N.B. Darshan may produce MULTIPLE output files for each of the actual HPC jobs (as it produces one per command
    that is run in the submit script).
    """
    # What fields are used by Darshan (that are different to the defaults in IngestedJob)
    uid = None
    nprocs = None
    jobid = None
    log_version = None

    def __init__(self, label=None, file_details=None, **kwargs):
        super(DarshanIngestedJob, self).__init__(label, **kwargs)

        assert file_details is not None
        self.file_details = file_details

    def aggregate(self, job):
        """
        Combine two ingested jobs together, as Darshan produces one file per command run inside the job script
        (and all these should be together).
        """
        assert self.label == job.label

        for filename, file_detail in job.file_details.iteritems():
            if filename in self.file_details:
                self.file_details[filename].aggregate(file_detail)
            else:
                self.file_details[filename] = file_detail

    def model_job(self, first_start_time):
        """
        Return a ModelJob from the supplied information
        """
        if float(self.log_version) <= 2.0:
            raise DarshanLogReaderError("Darshan log version unsupported")

        return ModelJob(
            time_start=self.time_start - first_start_time,
            duration=self.time_end - self.time_start,
            ncpus=self.nprocs,
            nnodes=1,
            time_series=self.model_time_series()
        )

    def model_time_series(self):
        """
        We want to model the time series here.

        TODO: Actually introduce time dependence. For now, it only considers totals!
        """
        total_read = 0
        total_written = 0
        total_reads = 0
        total_writes = 0

        for file in self.file_details.values():
            total_read += file.bytes_read
            total_written += file.bytes_written
            total_reads += file.read_count
            total_writes += file.write_count

        return {
            'kb_read': TimeSignal.from_values('kb_read', [0.0], [float(total_read) / 1024.0]),
            'kb_write': TimeSignal.from_values('kb_write', [0.0], [float(total_written) / 1024.0])

            # TODO: Make use of read/write counts
            # TimeSignal.from_values('n_read', [0.0], [float(total_reads)]),
            # TimeSignal.from_values('n_write', [0.0], [float(total_writes)]),
        }


class DarshanLogReader(LogReader):

    job_class = DarshanIngestedJob
    dataset_class = DarshanDataSet
    log_type_name = "Darshan"
    file_pattern = "*.gz"

    # By default we end up with a whole load of darshan logfiles within a directory.
    label_method = "directory"

    darshan_params = {
        'uid': ('uid', int),
        'jobid': ('jobid', int),
        'nprocs': ('nprocs', int),
        'start_time': ('time_start', int),
        'end_time': ('time_end', int),
        'darshan log version': ('log_version', str)
    }

    # See darshan summary on cca/ccb in the darshan module
    file_params = {
        'CP_BYTES_READ': 'bytes_read',
        'CP_BYTES_WRITTEN': 'bytes_written',
        'CP_POSIX_OPENS': 'open_count',
        'CP_POSIX_FOPENS': 'open_count',
        'CP_POSIX_READ_TIME': 'read_time',
        'CP_POSIX_WRITE_TIME': 'write_time',
        'CP_POSIX_WRITES': 'write_count',
        'CP_POSIX_FWRITES': 'write_count',
        'CP_POSIX_READS': 'read_count',
        'CP_POSIX_FREADS': 'read_count'

        # CP_SIZE_AT_OPEN
        # CP_MODE
        # CP_POSIX_FSEEKS, CP_POSIX_SEEKS
        # CP_POSIX_STATS
        # CP_POSIX_FSYNCS
        # CP_F_POSIX_META_TIME, CP_F_MPI_META_TIME
    }

    def __init__(self, path, **kwargs):

        # TODO: Configure the darshan paths (need darshan-parser)
        print "Darshan Log Reader"

        # Custom configuration:
        self.parser_command = kwargs.pop('parser', 'darshan-parser')

        super(DarshanLogReader, self).__init__(path, **kwargs)

    def read_log(self, filename, suggested_label):
        """
        Read a darshan log!
        """
        # try:
        #     output = subprocess.check_output([self.parser_command, filename])
        # except subprocess.CalledProcessError as e:

        pipes = subprocess.Popen([self.parser_command, filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = pipes.communicate()

        if len(error) != 0:
            print_colour("orange", "\n{}".format(error.strip()), flush=True)

        if pipes.returncode != 0:
            if len(error) == 0:
                print ""
            print_colour("red", "Got an error: {} - {}".format(pipes.returncode, filename), flush=True)

            # Just skip this (with warnings), as the Darshan data is only being used in conjunction with something
            # else, so there won't be a blank.
            return []

        files = {}
        params = {
            'label': suggested_label,
            'filename': filename
        }

        for line in output.splitlines():

            trimmed_line = line.strip()

            if len(trimmed_line) == 0:
                pass
            elif trimmed_line[0] == '#':
                split = trimmed_line.split(':', 1)
                key = split[0][1:].strip()
                job_key, key_type = self.darshan_params.get(key, (None, None))
                if job_key:
                    params[job_key] = key_type(split[1].split()[0].strip())
            else:
                # A data line
                bits = trimmed_line.split()
                # file = ' '.join(bits[4:])
                file = bits[4]

                # Add the file to the map if required
                if file not in files:
                    files[file] = DarshanIngestedJobFile(file)

                file_elem = self.file_params.get(bits[2], None)
                if file_elem is not None:
                    setattr(files[file], file_elem, getattr(files[file], file_elem) + int(bits[3]))

        return [self.job_class(file_details=files, **params)]

    def read_logs_generator(self):
        """
        Darshan produces one log per command executed in the script. This results in multiple Darshan files per
        job, which need to be aggregated. Each of the jobs will be sequential, so we combine them.
        """

        current_job = None

        for job in super(DarshanLogReader, self).read_logs_generator():

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

