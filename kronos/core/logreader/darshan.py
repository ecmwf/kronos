# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import subprocess

from kronos.core.jobs import IngestedJob, ModelJob
from kronos.core.logreader.base import LogReader
from kronos.core.logreader.dataset import IngestedDataSet
from kronos.core.time_signal import TimeSignal
from kronos.core.kronos_tools.merge import min_not_none, max_not_none
from kronos.core.kronos_tools.print_colour import print_colour

darshan_signal_priorities = {
    'kb_read': 8,
    'kb_write': 8,
    'n_read': 8,
    'n_write': 8,
}


class DarshanLogReaderError(Exception):
    pass


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

        self.open_time = None
        self.read_time_start = None
        self.read_time_end = None
        self.write_time_start = None
        self.write_time_end = None
        self.close_time = None

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

        self.open_time = min_not_none(self.open_time, other.open_time)
        self.read_time_start = min_not_none(self.read_time_start, other.read_time_start)
        self.read_time_end = max_not_none(self.read_time_end, other.read_time_end)
        self.write_time_start = min_not_none(self.write_time_start, other.write_time_start)
        self.write_time_end = max_not_none(self.write_time_end, other.write_time_end)
        self.close_time = max_not_none(self.close_time, other.close_time)

    def adjust_times(self, delta):
        """
        Shift the recorded times by a given offset
        """
        self.open_time = self.open_time + delta if self.open_time is not None else None
        self.read_time_start = self.read_time_start + delta if self.read_time_start is not None else None
        self.read_time_end = self.read_time_end + delta if self.read_time_end is not None else None
        self.write_time_start = self.write_time_start + delta if self.write_time_start is not None else None
        self.write_time_end = self.write_time_end + delta if self.write_time_end is not None else None
        self.close_time = self.close_time + delta if self.close_time is not None else None


class DarshanIngestedJob(IngestedJob):
    """
    N.B. Darshan may produce MULTIPLE output files for each of the actual HPC jobs (as it produces one per command
    that is run in the submit script).
    """
    # What fields are used by Darshan (that are different to the defaults in IngestedJob)
    exe_cmd = None
    uid = None
    nprocs = None
    jobid = None
    log_version = None

    def __init__(self, label=None, file_details=None, **kwargs):
        super(DarshanIngestedJob, self).__init__(label, **kwargs)

        assert file_details is not None
        self.file_details = file_details

    def aggregate(self, other):
        """
        Combine two ingested jobs together, as Darshan produces one file per command run inside the job script
        (and all these should be together).
        """
        assert self.label == other.label

        time_difference = other.time_start - self.time_start
        if time_difference < 0:
            self.time_start = other.time_start
            for file_detail in self.file_details.values():
                file_detail.adjust_times(-time_difference)
        else:
            for file_detail in other.file_details.values():
                file_detail.adjust_times(time_difference)

        for filename, file_detail in other.file_details.iteritems():
            if filename in self.file_details:
                self.file_details[filename].aggregate(file_detail)
            else:
                self.file_details[filename] = file_detail

    def model_job(self):
        """
        Return a ModelJob from the supplied information
        """
        if float(self.log_version) <= 2.0:
            raise DarshanLogReaderError("Darshan log version unsupported")

        return ModelJob(
                        job_name=self.filename,
                        user_name=None,  # not provided
                        cmd_str=self.exe_cmd,
                        queue_name=None,  # not provided
                        time_queued=None,  # not provided
                        time_start=self.time_start,
                        duration=self.time_end - self.time_start,
                        ncpus=self.nprocs,
                        nnodes=None,  # not provided
                        stdout=None,  # not provided
                        label=self.label,
                        timesignals=self.model_time_series(),
                        )

    def model_time_series(self):
        """
        We want to model the time series here.

        TODO: Actually introduce time dependence. For now, it only considers totals!
        """
        read_data = []
        read_counts = []
        write_data = []
        write_counts = []

        for model_file in self.file_details.values():

            if model_file.read_time_start is not None and (model_file.read_count != 0 or model_file.bytes_read != 0):
                read_data.append((model_file.read_time_start, model_file.bytes_read / 1024.0,
                                  model_file.read_time_end - model_file.read_time_end))
                read_counts.append((model_file.read_time_start, model_file.read_count,
                                    model_file.read_time_end - model_file.read_time_start))

            if model_file.write_time_start is not None and (model_file.write_count != 0 or model_file.bytes_written != 0):
                write_data.append((model_file.write_time_start, model_file.bytes_written / 1024.0,
                                   model_file.write_time_end - model_file.write_time_start))
                write_counts.append((model_file.write_time_start, model_file.write_count,
                                     model_file.write_time_end - model_file.write_time_start))

        times_read, read_data, read_durations = zip(*read_data) if read_data else (None, None, None)
        times_read2, read_counts, read_durations2 = zip(*read_counts) if read_counts else (None, None, None)
        times_write, write_data, write_durations = zip(*write_data) if write_data else (None, None, None)
        times_write2, write_counts, write_durations2 = zip(*write_counts) if write_counts else (None, None, None)

        time_series = {}
        if read_data:
            time_series['kb_read'] = TimeSignal.from_values('kb_read', times_read, read_data,
                                                            durations=read_durations,
                                                            priority=darshan_signal_priorities['kb_read'])
        if write_data:
            time_series['kb_write'] = TimeSignal.from_values('kb_write', times_write, write_data,
                                                             durations=write_durations,
                                                             priority=darshan_signal_priorities['kb_write'])
        if read_counts:
            time_series['n_read'] = TimeSignal.from_values('n_read', times_read, read_counts,
                                                           durations=read_durations,
                                                           priority=darshan_signal_priorities['n_read'])
        if write_counts:
            time_series['n_write'] = TimeSignal.from_values('n_write', times_write, write_counts,
                                                            durations=write_durations,
                                                            priority=darshan_signal_priorities['n_write'])

        return time_series


class DarshanLogReader(LogReader):

    job_class = DarshanIngestedJob
    log_type_name = "Darshan"
    file_pattern = "*.gz"
    recursive = True

    # By default we end up with a whole load of darshan logfiles within a directory.
    label_method = "directory"

    darshan_params = {
        'exe': ('exe_cmd', str),
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
       # 'CP_POSIX_READ_TIME': 'read_time',
       # 'CP_POSIX_WRITE_TIME': 'write_time',
        'CP_POSIX_WRITES': 'write_count',
        'CP_POSIX_FWRITES': 'write_count',
        'CP_POSIX_READS': 'read_count',
        'CP_POSIX_FREADS': 'read_count',

        "CP_F_OPEN_TIMESTAMP": "open_time",
        'CP_F_READ_START_TIMESTAMP': 'read_time_start',
        'CP_F_WRITE_START_TIMESTAMP': 'write_time_start',
        "CP_F_READ_END_TIMESTAMP": 'read_time_end',
        "CP_F_WRITE_END_TIMESTAMP": 'write_time_end',
        "CP_F_CLOSE_TIMESTAMP": 'close_time'



        # CP_SIZE_AT_OPEN
        # CP_MODE
        # CP_POSIX_FSEEKS, CP_POSIX_SEEKS
        # CP_POSIX_STATS
        # CP_POSIX_FSYNCS
        # CP_F_POSIX_META_TIME, CP_F_MPI_META_TIME
    }

    def __init__(self, path, **kwargs):

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

        return self._read_log_internal(output, filename, suggested_label)

    def _read_log_internal(self, parser_output, filename, suggested_label):

        files = {}
        params = {
            'label': suggested_label,
            'filename': filename
        }

        for line in parser_output.splitlines():

            trimmed_line = line.strip()

            if len(trimmed_line) == 0:
                pass
            elif trimmed_line[0] == '#':
                bits = trimmed_line.split(':', 1)
                parameter_key = bits[0][1:].strip()
                job_key, key_type = self.darshan_params.get(parameter_key, (None, None))
                if job_key:
                    if job_key == 'exe_cmd':
                        params[job_key] = key_type(bits[1].strip())
                    else:
                        params[job_key] = key_type(bits[1].split()[0].strip())
            else:
                # A data line
                bits = trimmed_line.split()
                # file = ' '.join(bits[4:])
                filename = bits[4]

                # Add the file to the map if required
                if filename not in files:
                    files[filename] = DarshanIngestedJobFile(filename)

                file_elem = self.file_params.get(bits[2], None)
                if file_elem is not None:
                    currval = getattr(files[filename], file_elem) or 0
                    setattr(files[filename], file_elem, currval + float(bits[3]))

        return [self.job_class(file_details=files, **params)]

    def read_logs(self):
        """
        Darshan produces one log per command executed in the script. This results in multiple Darshan files per
        job, which need to be aggregated. Each of the jobs will be sequential, so we combine them.
        """

        current_job = None

        for job in super(DarshanLogReader, self).read_logs():

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


class DarshanDataSet(IngestedDataSet):

    log_reader_class = DarshanLogReader

    def model_jobs(self):
        """
        Model the Darshan jobs, given a list of injested jobs

        """
        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        # global_start_time = min((j.time_start for j in self.joblist))

        for job in self.joblist:
            # yield job.model_job(global_start_time)
            yield job.model_job()


