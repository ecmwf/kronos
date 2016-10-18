import re
import datetime
import calendar

from jobs import IngestedJob, ModelJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet
from kronos_tools.print_colour import print_colour


class StdoutECMWFIngestedJob(IngestedJob):
    """
    N.B. Darshan may produce MULTIPLE output files for each of the actual HPC jobs (as it produces one per command
    that is run in the submit script).
    """
    # What fields are used by IPM (that are different to the defaults in IngestedJob)

    node_tasks = None
    task_threads = None
    tasks = None
    duration = None
    max_node_threads = None
    hyperthreads = None

    def model_job(self, global_start_time):
        """
        Return a ModelJob from the supplied information
        """
        try:
            time_start = calendar.timegm(self.time_start.timetuple())
            time_end = calendar.timegm(self.time_end.timetuple())
            time_queued = calendar.timegm(self.time_created.timetuple())

            assert self.duration == (time_end - time_start)

            max_cpus = self.nnodes * self.max_node_threads / self.hyperthreads
            threads = self.tasks * self.task_threads / self.hyperthreads

        except Exception as e:
            print "Rethrowing exception {}".format(e)
            print "Error in job: {}".format(self.label)
            print "Filename: {}".format(self.filename)
            raise

        # TODO: We want to capture multi-threading as well as multi-processing somewhere3
        return ModelJob(
            scheduler_timing=True,
            label=self.label,
            time_start=time_queued - global_start_time,
            duration=self.duration,
            ncpus=min(threads, max_cpus),
            nnodes=self.nnodes
        )


class StdoutECMWFLogReader(LogReader):

    job_class = StdoutECMWFIngestedJob
    log_type_name = "stdout (ECMWF)"
    file_pattern = "*.1"
    recursive = True

    # By default we end up with a whole load of darshan logfiles within a directory.
    label_method = "directory-file-root"

    parse_time = lambda s: datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %Y")
    info_fields = {
        "Queued":                  {"field": "time_created",     "type": parse_time},
        "Dispatched":              {"field": "time_start",       "type": parse_time},
        "Completed":               {"field": "time_end",         "type": parse_time},
        "Runtime":                 {"field": "duration",         "type": lambda s: int(s.split()[0])},
        "Owner":                   {"field": "user",             "type": str},
        "EC_nodes":                {"field": "nnodes",           "type": int},
        "EC_threads_per_task":     {"field": "task_threads",     "type": int},
        "EC_tasks_per_node":       {"field": "node_tasks",       "type": int},
        "EC_total_tasks":          {"field": "tasks",            "type": int},
        "EC_hyperthreads":         {"field": "hyperthreads",     "type": int},
        "EC_max_threads_per_node": {"field": "max_node_threads", "type": int},
    }

    # could also match #PBS -q np, etc.
    re_infoline = re.compile("## INFO (.*) : (.*)")
    re_directive = re.compile("## INFO .*#PBS -l (.*)=(.*)")
    re_env_summary = re.compile("(EC_[a-zA-Z0-9_]*)=([^ ]+)")

    def read_log(self, filename, suggested_label):
        """
        Read stdout from one of the jobs. We want to capture the pro/epilogues.
        """
        attrs = {
            'label': suggested_label,
            'filename': filename
        }
        have_error = False

        with open(filename, 'r') as f:
            for line in f:

                # match the various sorts of acceptable lines.
                m = self.re_infoline.match(line)
                if not m:
                    m = self.re_directive.match(line)
                if not m:
                    m = self.re_env_summary.match(line)

                if m:
                    field = self.info_fields.get(m.groups()[0].strip())
                    if field:
                        fieldname = field['field']
                        fieldval = field['type'](m.groups()[1])
                        if fieldname in attrs and attrs[fieldname] != fieldval:
                            # Don't raise an exception here. Not good to kill ingestion due to one bad file...
                            # raise IngestionError("Fieldname {} already has value {}, not {} for file {}".format(
                            #     fieldname, attrs[fieldname], fieldval, filename))

                            # For some reason, we occasionally get EC_nodes and EC_nodes_total output into the summary
                            # with differing values. Don't fret about that...
                            if fieldname == "nnodes":
                                # We want to store the larger of the values, so only let the larger one through.
                                if fieldval < attrs[fieldname]:
                                    continue
                            else:

                                # Keep track of if this is the first error printed for this file. If it is, we want to
                                # start a newline so it doesn't interfere with the printing of the progress.
                                if not have_error:
                                    print ""
                                print_colour("orange", "Fieldname {} already has value {}, not {} for file {}".format(
                                    fieldname, attrs[fieldname], fieldval, filename))
                                have_error = True

                        attrs[fieldname] = fieldval

        # Check that we have enough data to be useful.
        required = [
            'time_start',
            'time_end',
            'time_created',
            'nnodes',
            'max_node_threads',
            'hyperthreads',
            'tasks',
            'task_threads'
        ]
        valid = True
        for field in required:
            if attrs.get(field, None) is None:
                valid = False
                if not have_error:
                    print ""
                print_colour("red", "Required field {} not found in log file {}".format(field, filename))
                have_error = True

        if valid:
            return [StdoutECMWFIngestedJob(**attrs)]
        else:
            return []


class StdoutECMWFDataSet(IngestedDataSet):

    log_reader_class = StdoutECMWFLogReader

    def model_jobs(self):
        """
        Model the IPM jobs, given a list of injested jobs
        """
        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        global_start_time = min((calendar.timegm(j.time_created.timetuple()) for j in self.joblist))

        for job in self.joblist:
            yield job.model_job(global_start_time)
