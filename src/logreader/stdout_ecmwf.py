import re
import datetime
import calendar

from jobs import IngestedJob, ModelJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet
from tools.print_colour import print_colour


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
        time_start = calendar.timegm(self.time_start.timetuple())
        time_end = calendar.timegm(self.time_end.timetuple())
        time_queued = calendar.timegm(self.time_created.timetuple())

        assert self.duration == (time_end - time_start)

        max_cpus = self.nnodes * self.max_node_threads / self.hyperthreads
        threads = self.tasks * self.task_threads / self.hyperthreads

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

    def read_log(self, filename, suggested_label):
        """
        Read stdout from one of the jobs. We want to capture the pro/epilogues.
        """
        with open(filename, 'r') as f:

            info_lines = [l for l in f if (len(l) > 7 and l[0:7] == "## INFO")]

        attrs = {
            'label': suggested_label,
            'filename': filename
        }

        for line in info_lines:
            m = self.re_infoline.match(line)
            if not m:
                m = self.re_directive.match(line)
            if m:
                field = self.info_fields.get(m.groups()[0].strip())
                if field:
                    fieldname = field['field']
                    fieldval = field['type'](m.groups()[1])
                    if fieldname in attrs and attrs[fieldname] != fieldval:
                        # Don't raise an exception here. Not good to kill ingestion due to one bad file...
                        # raise IngestionError("Fieldname {} already has value {}, not {} for file {}".format(
                        #     fieldname, attrs[fieldname], fieldval, filename))
                        print_colour("red", "Fieldname {} already has value {}, not {} for file {}".format(
                            fieldname, attrs[fieldname], fieldval, filename)))
                    attrs[fieldname] = fieldval

        return [StdoutECMWFIngestedJob(**attrs)]


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
