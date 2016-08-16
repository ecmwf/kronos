"""
A dataset describes a series of profiling data elements from a data source

In particular, it has a list of IngestedJobs, and some metadata that permits
those to be processed.
"""
# Try to use cPickle rather than pickle, by default, as it is MUCH faster. Fallback to pickle if it is
# not available on a given platform, or other issues.
try:
    import cPickle as pickle
except:
    import pickle

import base64
import errno
import os

from exceptions_iows import ConfigurationError
from tools.print_colour import print_colour


class IngestedDataSet(object):
    """
    This is the base class. It contains only blank data, and a static factory routine
    to enable further processing.
    """
    log_reader_class = None

    def __init__(self, joblist, ingest_path, ingest_config):
        # assert isinstance(joblist, list)
        self.joblist = list(joblist)
        self.ingest_path = ingest_path
        self.ingest_config = ingest_config

    def __unicode__(self):
        return "Dataset({}) - {} jobs".format(self.__class__.__name__, len(self.joblist))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def model_jobs(self):
        """
        Model the jobs (given a list of injested jobs)

        -- Override this routine to do any log-reader-specific global processing (e.g. adjusting for
           global start times, ...).
        """
        for job in self.joblist:
            yield job.model_job()


    @classmethod
    def from_logs_path(cls, ingest_path, ingest_config):
        """
        This method should construct a log reader, read the logs and return an IngestedDataSet.

        If the logs are cached, then those should be read in instead.
        """
        abs_ingest_path = os.path.abspath(os.path.realpath(ingest_path))
        cache_file = "cache.{}".format(base64.b64encode(abs_ingest_path))
        dataset = None

        # Remove reparse from the dictionary, so it is never used to compare validity of cached files.
        print ingest_config
        reparse = ingest_config.pop('reparse', False)
        cache = ingest_config.pop('cache', True)

        if not reparse:

            try:
                with open(cache_file, 'r') as f:
                    print "Using cached data from: {}".format(f.name)
                    dataset = pickle.load(f)

            except (IOError, OSError) as e:
                if e.errno == errno.ENOENT:
                    print "No cache file found for ingest path"
                else:
                    # An actual file read error occurred. Throw back to the user.
                    raise

            if dataset:

                if dataset.ingest_config != ingest_config:
                    print_colour("red", "Log reader configuration doesn't match cache file")
                    print_colour("orange", "Reader: {}".format(ingest_config))
                    print_colour("orange", "Cached: {}".format())
                    print_colour("green", "Please modify configuration, or delete cache file and try again")
                    raise ConfigurationError("Log reader configuration doesn't match cache file")

                if os.path.abspath(os.path.realpath(dataset.ingest_path)) != abs_ingest_path:
                    raise ConfigurationError("Ingestion path in cache file does not match ingestion path")

        if dataset is None:

            # Finally read the logs, if that is required
            lr = cls.log_reader_class(ingest_path, **ingest_config)
            dataset = cls(lr.read_logs(), ingest_path, ingest_config)

            # Pickle the object for later rapid loading.
            if cache:
                print "Writing cache file: {}".format(cache_file)
                with open(cache_file, "w") as f:
                    pickle.dump(dataset, f)

        return dataset
