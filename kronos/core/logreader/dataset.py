# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

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
import datetime

from kronos.core.exceptions_iows import ConfigurationError
from kronos.core.kronos_tools.print_colour import print_colour


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

    def apply_cutoff_dates(self, cutoff_date_0=None, cutoff_date_1=None):
        """
        apply cut-off dates to the dataset:

         -- only jobs started within he considered interval will be retained..
        """

        # throw an error if neither dates have been specified
        if cutoff_date_0 is None and cutoff_date_1 is None:
            ValueError("neither cut-off dates have been specified!")

        if datetime.datetime(*cutoff_date_0) >= datetime.datetime(*cutoff_date_1):
            ValueError("first cut-off date should be < second cut-off date!")

        if cutoff_date_0:
            assert isinstance(cutoff_date_0, list)

        if cutoff_date_1:
            assert isinstance(cutoff_date_1, list)

        cut_jobs = []
        for job in self.joblist:

            # make sure time stamp is a datetime type
            job_start_time = job.time_start
            if isinstance(job_start_time, float) or isinstance(job_start_time, int):
                job_start_time = datetime.datetime.fromtimestamp(job.time_start)

            # cut-off the jobs as appropriate
            if (cutoff_date_0 is not None) and (cutoff_date_1 is None):
                if job_start_time >= datetime.datetime(*cutoff_date_0):
                    cut_jobs.append(job)
            elif (cutoff_date_0 is None) and (cutoff_date_1 is not None):
                if job_start_time <= datetime.datetime(*cutoff_date_1):
                    cut_jobs.append(job)
            elif (cutoff_date_0 is not None) and (cutoff_date_1 is not None):
                if datetime.datetime(*cutoff_date_0) <= job_start_time <= datetime.datetime(*cutoff_date_1):
                    cut_jobs.append(job)

        self.joblist = cut_jobs

        return self

    @classmethod
    def from_pickled(cls, ingest_file):
        print "ingesting {}".format(ingest_file)
        with open(ingest_file, 'r') as f:
            return pickle.load(f)


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

    def export_time_series(self, param_name):
        """
        Export dataset-specific quantities
        :return:
        """
        raise NotImplementedError("export function not implemented for class: {}".format(type(self)))
