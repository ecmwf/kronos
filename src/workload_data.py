from kpf_handler import KPFFileHandler
from kronos_tools import print_colour


class WorkloadData(object):
    """
    Workload data contains "workloads", each composed of standardized "model jobs"
    """

    default_tag = 0

    def __init__(self):

        # each workload is a dictionary {'tag': <>, 'jobs': <list-of-model_jobs>}
        self.workloads = []

    def __unicode__(self):
        return "WorkloadData - {} job sets".format(len(self.workloads))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def verbose_description(self):
        """
        A verbose output for the modelled workload
        :return:
        """
        output = "=============================================\n"
        output += "Verbose workload description: {}\n".format(self)

        for set in self.workloads:
            for i, job in enumerate(set['jobs']):
                output += '--\nJob {}:\n'.format(i)
                output += job.verbose_description()

        output += "=============================================\n"
        return output

    def append_workload(self, model_jobs=None, set_tag=None):
        """
        from a list of modelled jobs and a tag
        :param model_jobs:
        :param set_tag:
        :return:
        """
        if not set_tag:
            set_tag = 'set'+str(WorkloadData.default_tag + 1)
            print_colour.print_colour('orange', '"tag" not specified, given default: {}'.format(set_tag))

        if not model_jobs:
            print_colour.print_colour('orange', 'set {} is an empty set:'.format(set_tag))
            self.workloads.append({
                                   'jobs': [],
                                   'tag': set_tag
                                   })
        else:
            self.workloads.append({
                                   'jobs': model_jobs,
                                   'tag': set_tag
                                   })

    def export_workloads(self, kpf_filename=None):
        """
        Export this workload to a kpf file
        :param kpf_filename:
        :return:
        """

        if not kpf_filename:
            kpf_filename = 'output.kpf'
        else:
            if not kpf_filename.endswith('.kpf'):
                print("extension .ksp will be appended")
                kpf_filename += '.kpf'

        # save a kpf file
        KPFFileHandler().save_kpf(self.workloads, kpf_filename)
