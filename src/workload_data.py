# from kpf_handler import KPFFileHandler
from kronos_tools import print_colour


class WorkloadData(object):
    """
    Workload data contains "jobs", each composed of standardized "model jobs"
    """

    tag_default = 0

    def __init__(self, jobs=None, tag=None):

        # a workload data essentially contains a list of model jobs + tag
        self.jobs = jobs if jobs else None
        self.tag = tag if tag else str(WorkloadData.tag_default+1)

    def __unicode__(self):
        return "WorkloadData - {} job sets".format(len(self.jobs))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def append_jobs(self, model_jobs=None):
        """
        from a list of modelled jobs and a tag
        :param model_jobs:
        :param set_tag:
        :return:
        """
        if not model_jobs:
            print_colour.print_colour('orange', 'provided an empty set oj model jobs!')
        else:
            self.jobs.append(model_jobs)
