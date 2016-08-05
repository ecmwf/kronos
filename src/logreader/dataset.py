"""
A dataset describes a series of profiling data elements from a data source

In particular, it has a list of IngestedJobs, and some metadata that permits
those to be processed.
"""


class IngestedDataSet(object):
    """
    This is the base class. It contains only blank data, and a static factory routine
    to enable further processing.
    """
    log_reader_class = None

    def __init__(self, joblist):
        # assert isinstance(joblist, list)
        self.joblist = list(joblist)

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
        lr = cls.log_reader_class(ingest_path, **ingest_config)

        return cls(lr.read_logs())
