# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import subprocess
from collections import OrderedDict

from kronos.kronos_modeller.jobs import IngestedJob, ModelJob
from kronos.kronos_modeller.logreader.base import LogReader
from kronos.kronos_modeller.logreader.dataset import IngestedDataSet
from kronos.kronos_modeller.time_signal.time_signal import TimeSignal
from kronos.kronos_modeller.tools.merge import min_not_none, max_not_none

from kronos_modeller.tools.print_colour import print_colour

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

    # JobFile parameters: init values behaviour when accumulated
    param_map = {
        "bytes_read":       {"init": 0,    "func": lambda x, y: x + y},
        "bytes_written":    {"init": 0,    "func": lambda x, y: x + y},
        "open_count":       {"init": 0,    "func": lambda x, y: x + y},
        "write_count":      {"init": 0,    "func": lambda x, y: x + y},
        "read_count":       {"init": 0,    "func": lambda x, y: x + y},
        "open_time":        {"init": None, "func": min},
        "read_time_start":  {"init": None, "func": min},
        "read_time_end":    {"init": None, "func": max},
        "write_time_start": {"init": None, "func": min},
        "write_time_end":   {"init": None, "func": max},
        "close_time":       {"init": None, "func": max},
    }

    def __init__(self, name):

        self.name = name

        # init accumulators and timestamps parameters
        for p_name, p_descr in self.param_map.items():
            setattr(self, p_name, p_descr["init"])

    def __unicode__(self):
        return "DarshanFile(times: [{:.6f}, {:.6f}]: {} reads, {} bytes, {} writes, {} bytes)".format(
            self.open_time, self.close_time, self.read_count, self.bytes_read, self.write_count, self.bytes_written)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def update_parameter(self, val_name, new_value):
        """
        This function updates values according to their type (accumulators or timestamps)
        :param val_name:
        :param new_value:
        :return:
        """

        # Update values only if the new value is different from zero:
        #  - byte counters = 0 are to be ignored
        #  - time stamps = 0 are not valid Darshan entries (eq to "value not found")
        if float(new_value):

            _init_when_hit = {
                min: float("inf"),
                max: 0,
            }

            _param_func = self.param_map[val_name]["func"]
            currval = getattr(self, val_name) or _init_when_hit.get(_param_func, 0)
            setattr(self, val_name, _param_func(currval, float(new_value)))

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

    def define_behaviour(self):
        """
        Return the "behaviour" of this file, options: [read_only, write_only, open_only, read_write]

        :return:
        """

        if self.bytes_written and not self.bytes_read:
            return "file_write_only"
        elif self.bytes_read and not self.bytes_written:
            return "file_read_only"
        elif self.bytes_written and self.bytes_written:
            return "file_read_write"
        elif not self.bytes_read and not self.bytes_written:
            return "file_open_only"
        else:
            raise LookupError("behaviour not understood.. I should not be here!")

    def time_stamped_operation(self, param_name):
        """
        This function returns the timestamp and the value of a parameter according to it's nature
          - e.g. if it's a write operation or read operation the timestamp differ..
        :return:
        """

        # Check that the request is valid
        assert param_name in ["bytes_read", "read_count", "bytes_written", "write_count"]

        if param_name in ["bytes_read", "read_count"] and self.read_time_start:

            return (self.read_time_start+self.read_time_end)/2.0, getattr(self, param_name)

        elif param_name in ["bytes_written", "write_count"] and self.write_time_start:

            return (self.write_time_start + self.write_time_end) / 2.0, getattr(self, param_name)

        else:
            return None


# /////////////////////////// Darshan version 2 classes ///////////////////////////////
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
        print "aggregating job: {}".format(self.label)

        time_difference = other.time_start - self.time_start
        if time_difference < 0:
            self.time_start = other.time_start
            for file_detail in self.file_details.values():
                file_detail.adjust_times(-time_difference)
        else:
            for file_detail in other.file_details.values():
                file_detail.adjust_times(time_difference)

        # update the time end
        self.time_end = max(self.time_end, other.time_end)

        for filename, file_detail in other.file_details.iteritems():
            if filename in self.file_details:
                self.file_details[filename].aggregate(file_detail)
            else:
                self.file_details[filename] = file_detail

    def summary_report(self, mode="summary"):
        """
        This function prints a summary of this darshan file
        :return:
        """

        _header = _fields = None

        if mode=="summary":
            summary_data = OrderedDict([
                ("write_count", 0),
                ("bytes_written", 0),
                ("read_count", 0),
                ("bytes_read", 0),
                ("open_count", 0),
                ("close_count", 0),
              ])

            # print summary data of this ingested job
            for fk, fv in self.file_details.iteritems():
                summary_data[fv.define_behaviour()] = summary_data.get(fv.define_behaviour(), 0) + 1
                for sk, sv in summary_data.items():
                    summary_data[sk] += getattr(fv, sk, 0)

            # print out the summary
            print_order = ["read_count", "bytes_read", "write_count", "bytes_written"]
            _header = " "*10 + " ".join(["{:^14}|".format(k) for k in ("#read", "bytes_read", "#write", "bytes_write", "job_name")])
            _fields = "[summary]" + " ".join(["{:15}".format(summary_data[k]) for k in print_order]) + " {:<15}".format(self.label)

        # write time series of operations
        elif mode == "time_series":
            time_series = []
            for fk, fv in self.file_details.iteritems():
                time_series.append(
                    [fv.open_time,
                     fv.close_time if fv.close_time else -1,
                     fv.read_count,
                     fv.bytes_read,
                     fv.write_count,
                     fv.bytes_written,
                     fv.name]
                )

            time_series.sort(key=lambda x: x[0])

            _header = "     "+" ".join(["{:^14}|".format(k) for k in ("t_first_open",
                                                                      "t_last_close",
                                                                      "#read",
                                                                      "bytes_read",
                                                                      "#write",
                                                                      "bytes_write",
                                                                      "file_name")])
            _fields = "\n".join(["[ts] "+" ".join("{:15}".format(v) for v in ts_entry) for ts_entry in time_series])

        return _header, _fields

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

        if self.time_end and self.time_start:
            duration = self.time_end - self.time_start
        else:
            duration = None

        for model_file in self.file_details.values():

            if duration:
                if model_file.read_time_start > duration:
                    model_file.read_time_start = duration - 1
                if model_file.read_time_end > duration:
                    model_file.read_time_end = duration

            if model_file.read_time_start is not None and (model_file.read_count != 0 or model_file.bytes_read != 0):
                read_data.append((model_file.read_time_start, model_file.bytes_read / 1024.0,
                                  model_file.read_time_end - model_file.read_time_start))
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
        self.parser_command = kwargs.pop('parser', None)

        # **Note**: if the parser command is not provided, **it assumes that the file is already parsed**
        #  => it will only "cat" the file
        self.parser_command = self.parser_command if self.parser_command else "cat"
        if self.parser_command == "cat":
            print "INFO: Darshan parser-command not provided, I assume the file has already been parsed.."

        super(DarshanLogReader, self).__init__(path, **kwargs)

    def read_log(self, filename, suggested_label):
        """
        Read a darshan log!
        """

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
                # print bits
                filename = bits[4]

                # Add the file to the map if required
                if filename not in files:
                    files[filename] = DarshanIngestedJobFile(filename)

                file_elem = self.file_params.get(bits[2], None)
                if file_elem is not None:
                    files[filename].update_parameter(file_elem, bits[3])

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

    def export_time_series(self, param_name):
        """
        This class generates a dataset-specific export of quantities
        :return:
        """

        list_ops = [(f.time_stamped_operation(param_name)[0]+job.time_start, f.time_stamped_operation(param_name)[1])
                    for job in self.joblist for f in job.file_details.values() if f.time_stamped_operation(param_name)]

        t_min = min(zip(*list_ops)[0])

        return sorted([(t-t_min, v) for t, v in list_ops], key=lambda x: x[0])


# /////////////////////////// Darshan version 3 classes ///////////////////////////////
class DarshanLogReader3(LogReader):

    job_class = DarshanIngestedJob
    log_type_name = "Darshan"
    file_pattern = "*.darshan"
    recursive = True

    # By default we end up with a whole load of darshan logfiles within a directory.
    label_method = "directory"

    files_to_filter_out = [
        "statistics.kresults",
        "<STDOUT>"
    ]

    darshan_params = {
        'exe': ('exe_cmd', str),
        'uid': ('uid', int),
        'jobid': ('jobid', int),
        'nprocs': ('nprocs', int),
        'start_time': ('time_start', int),
        'end_time': ('time_end', int),
        'darshan log version': ('log_version', str)
    }

    # File parameters for the synthetic apps profiled with Darshan v3.11
    file_params = {
        'STDIO_BYTES_READ': 'bytes_read',
        'POSIX_BYTES_READ': 'bytes_read',

        'STDIO_BYTES_WRITTEN': 'bytes_written',
        'POSIX_BYTES_WRITTEN': 'bytes_written',

        'STDIO_OPENS': 'open_count',
        'POSIX_OPENS': 'open_count',

        'STDIO_WRITES': 'write_count',
        'POSIX_WRITES': 'write_count',

        'STDIO_READS': 'read_count',
        'POSIX_READS': 'read_count',

        "STDIO_F_OPEN_START_TIMESTAMP": "open_time",
        "POSIX_F_OPEN_START_TIMESTAMP": "open_time",

        'STDIO_F_READ_START_TIMESTAMP': 'read_time_start',
        'POSIX_F_READ_START_TIMESTAMP': 'read_time_start',

        'STDIO_F_WRITE_START_TIMESTAMP': 'write_time_start',
        'POSIX_F_WRITE_START_TIMESTAMP': 'write_time_start',

        "STDIO_F_READ_END_TIMESTAMP": 'read_time_end',
        "POSIX_F_READ_END_TIMESTAMP": 'read_time_end',

        "STDIO_F_WRITE_END_TIMESTAMP": 'write_time_end',
        "POSIX_F_WRITE_END_TIMESTAMP": 'write_time_end',

        "STDIO_F_CLOSE_END_TIMESTAMP": 'close_time',
        "POSIX_F_CLOSE_END_TIMESTAMP": 'close_time'
    }

    def __init__(self, path, **kwargs):

        # Custom configuration:
        self.parser_command = kwargs.pop('parser', 'darshan-parser')

        super(DarshanLogReader3, self).__init__(path, **kwargs)

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
                filename = bits[5]

                # Add the file to the map if required
                if not any([fname in filename for fname in self.files_to_filter_out]):

                    if filename not in files:
                        files[filename] = DarshanIngestedJobFile(filename)

                    file_elem = self.file_params.get(bits[3], None)
                    if file_elem is not None:
                        currval = getattr(files[filename], file_elem) or 0
                        setattr(files[filename], file_elem, currval + float(bits[4]))

        return [self.job_class(file_details=files, **params)]

    def read_logs(self):
        """
        Darshan produces one log per command executed in the script. This results in multiple Darshan files per
        job, which need to be aggregated. Each of the jobs will be sequential, so we combine them.
        """

        current_job = None

        for job in super(DarshanLogReader3, self).read_logs():

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


class Darshan3DataSet(IngestedDataSet):
    """
    Darshan dataset extracted from darshan v3
    """

    log_reader_class = DarshanLogReader3

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

