import fnmatch
import os
import sys
from multiprocessing import Pool

from exceptions_iows import ConfigurationError
from jobs import IngestedJob


# TODO: Tidy this up. The work should be done inside the class. Global state is ugly.

global_storage = None

def internal_worker(filename_tuple):
    i, filename = filename_tuple

    # print "WORKER: ", i, filename

    # In case some really whacky stuff is passed in, this is not the exception we would choose to throw
    try:
        p = os.path.basename(filename)
    except:
        p = filename

    # if i % 17 == 0:
    if True:
        sys.stdout.write("\r{:d} files processed - {:100s}".format(i+1, str(p)))
        sys.stdout.flush()

    return global_storage.read_log(filename, global_storage.suggest_label(filename))


def internal_initializer(args):

    print "Initialising worker!!!"
    global global_storage
    global_storage = args


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
    pool_readers = 10

    # How may the job be labelled (for combining with other profiling data, automatically)
    available_label_methods = [
        None,
        'directory'
    ]

    def __init__(self, path, recursive=None, file_pattern=None, label_method=None, pool_readers=None):
        self.path = path

        self.label_method = label_method if label_method is not None else self.label_method
        self.recursive = recursive if recursive is not None else self.recursive
        self.pool_readers = pool_readers if pool_readers is not None else self.pool_readers

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
        pool = Pool(processes=self.pool_readers, initializer=internal_initializer, initargs=(self,))
        self.stdout = sys.stdout

        list_of_job_lists = pool.imap_unordered(internal_worker, enumerate(self.logfiles()))

        for job_list in list_of_job_lists:
            for job in job_list:
                yield job

        sys.stdout.write("\n")

    def read_logs(self):
        """
        Read all of the logfiles which match the configuration.
        """
        # return self.dataset_class(itertools.chain(*(self.read_log(filename, self.suggest_label(filename)) for filename in self.logfiles())))

        return self.dataset_class(self.read_logs_generator())

