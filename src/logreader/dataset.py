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
    def __init__(self, joblist):
        # assert isinstance(joblist, list)
        self.joblist = list(joblist)

    def __unicode__(self):
        return "Dataset({}) - {} jobs".format(self.__class__.__name__, len(self.joblist))

    def __str__(self):
        return unicode(self).encode('utf-8')

