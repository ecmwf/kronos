import fnmatch
import os
import itertools

from jobs import IngestedJob


class LogReader(object):
    """
    A base class for all log readers

    --> Provide file iteration and dataset handling.
    """
    job_class = IngestedJob
    dataset_class = None
    file_pattern = None

    def __init__(self, path, recursive=False, file_pattern=None):
        self.path = path
        self.recursive = recursive

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

    def read_log(self, filename):
        raise NotImplementedError

    def read_logs(self):
        """
        Read all of the logfiles which match the configuration.
        """
        return self.dataset_class(itertools.chain(*(self.read_log(filename) for filename in self.logfiles())))

