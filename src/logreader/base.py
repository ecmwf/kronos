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

    def logfiles(self):
        """
        If the specified path is an (existing) file, then just return that file. If it is a directory, then
        iterate through the matching
        :return:
        """
        raise NotImplementedError
        yield ""

    def read_log(self, filename):
        raise NotImplementedError

    def read_logs(self):
        """
        Read all of the logfiles which match the configuration.
        """
        return self.dataset_class((self.read_log(filename) for filename in self.logfiles()))

