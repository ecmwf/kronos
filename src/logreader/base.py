import fnmatch
import os
import sys
import itertools

from exceptions_iows import ConfigurationError
from jobs import IngestedJob


class LogReader(object):
    """
    A base class for all log readers

    --> Provide file iteration and dataset handling.
    """
    job_class = IngestedJob
    dataset_class = None
    file_pattern = None
    label_method = None
    recursive = False

    # How may the job be labelled (for combining with other profiling data, automatically)
    available_label_methods = [
        None,
        'directory'
    ]

    def __init__(self, path, recursive=None, file_pattern=None, label_method=None):
        self.path = path

        self.label_method = label_method if label_method is not None else self.label_method
        self.recursive = recursive if recursive is not None else self.recursive

        # Some checks
        if self.label_method not in self.available_label_methods:
            raise ConfigurationError("Configuring LogReader with unavailable label method ({})".format(label_method))

        # Only override the file pattern if it is supplied.
        if file_pattern:
            self.file_pattern = file_pattern

    def __unicode__(self):
        return "LogReader({})[{}]".format(self.__class__.__name__, self.path)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def logfiles(self):
        """
        If the specified path is an (existing) file, then just return that file. If it is a directory, then
        iterate through the matching
        :return:
        """
        print "Iterating logfiles"
        if not os.path.exists(self.path):
            raise IOError("Path {} does not exist (specified for {})".format(self.path, self))

        if os.path.isfile(self.path):
            # If the specified path is a file, then it should be directly returned
            yield self.path

        elif self.recursive:
            # Find files in the specified path RECURSIVELY, matching the file pattern if relevant
            for root, dirs, files in os.walk(self.path):
                pattern = os.path.join(self.path, self.file_pattern) if self.file_pattern else None
                for filename in files:
                    full_filename = os.path.join(root, filename)
                    if pattern is None or fnmatch.fnmatch(full_filename, pattern):
                        yield full_filename

        else:
            # Find files in the specified path (non-recursively), matching the file pattern if relevant
            pattern = os.path.join(self.path, self.file_pattern) if self.file_pattern else None
            for filename in os.listdir(self.path):
                fn = os.path.join(self.path, filename)
                if os.path.isfile(fn) and (pattern is None or fnmatch.fnmatch(fn, pattern)):
                    yield fn

    def suggest_label(self, filename):
        """
        Do we have a suggested label?
        """
        if self.label_method == "directory":
            return os.path.dirname(filename)

        return None

    def read_log(self, filename, suggested_label):
        raise NotImplementedError

    def read_logs_generator(self):
        """
        This routine is an internal part of read_logs, and iterates through read_log for each job
        """
        # This should be roughly equivalent to:

        for i, filename in enumerate(self.logfiles()):

            # In case some really whacky stuff is passed in, this is not the exception we would choose to throw
            try:
                p = os.path.basename(filename)
            except:
                p = filename

            # if i % 17 == 0:
            if True:
                sys.stdout.write("\r{:d} files processed - {:100s}".format(i+1, str(p)))
                sys.stdout.flush()

            ingested_jobs = self.read_log(filename, self.suggest_label(filename))

            for job in ingested_jobs:
                yield job

        sys.stdout.write("\n")

    def read_logs(self):
        """
        Read all of the logfiles which match the configuration.
        """
        # return self.dataset_class(itertools.chain(*(self.read_log(filename, self.suggest_label(filename)) for filename in self.logfiles())))

        return self.dataset_class(self.read_logs_generator())

