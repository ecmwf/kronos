import fnmatch
import os
import sys

from exceptions_iows import ConfigurationError
from jobs import IngestedJob
from tools.process_pool import ProcessingPool


class LogReader(object):
    """
    A base class for all log readers

    --> Provide file iteration and dataset handling.
    """
    job_class = IngestedJob
    log_type_name = "(Unknown)"
    file_pattern = None
    label_method = None
    recursive = False
    pool_readers = 10

    # How may the job be labelled (for combining with other profiling data, automatically)
    available_label_methods = [
        None,
        'directory',
        'directory-no-par-serial'
    ]

    def __init__(self, path, recursive=None, file_pattern=None, label_method=None, pool_readers=None):
        self.path = path

        print "Log reader ({})".format(self.log_type_name)

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
        if self.label_method == "directory" or self.label_method == "directory-no-par-serial":

            # Remove the common base from the files, so that they can be compared from different ingestion sources.
            label = os.path.relpath(os.path.dirname(filename), self.path)

            # If the basename is "parallel" or "serial", remove it.
            if self.label_method == "directory-no-par-serial":
                basename = os.path.basename(label)
                if basename == "parallel" or basename == "serial":
                    label = os.path.dirname(label)

            return label

        return None

    def read_log(self, filename, suggested_label):
        raise NotImplementedError

    @staticmethod
    def _read_log_wrapper(filename, reader):
        return reader.read_log(filename, reader.suggest_label(filename))

    @staticmethod
    def _progress_printer(completed_count, element_num, read_log_output):
        try:
            p = os.path.basename(read_log_output[0].filename)

        except IndexError:
            # If an empty processing list has been returned, then skip printing
            return

        except:
            # If something whacky goes wrong, we don't want to be raising an exception based on this
            p = "Unknown"

        sys.stdout.write("\r{:d} files processed - {:100s}".format(completed_count, str(p)))
        sys.stdout.flush()

    def read_logs(self):
        """
        Iterate through read_log for each job

        --> Returns a generator of job objects, depending on the list of files to parse from self.logfiles()
        """
        print "Reading {} logs using {} workers".format(self.log_type_name, self.pool_readers)

        # n.b. There are constraints on what can be passed as arguments to this routine. The use of global data in
        #      the processing pool, and the use of static functions, is to (a) minimise the data being transferred
        #      between processes using the multiprocessing functionality, and (b) ensure that all of the transferred
        #      data can be pickled (which is required).

        pool = ProcessingPool(
            self._read_log_wrapper, # Read the log. Note that "self" is passed to this as an argument
            self._progress_printer, # Callback called after each element is processed
            processes=self.pool_readers,
            global_data=self)

        list_of_job_lists = pool.imap(self.logfiles())

        for job_list in list_of_job_lists:
            for job in job_list:
                yield job

        sys.stdout.write("\n")


