from kronos_tools.print_colour import print_colour


class WorkloadFiller(object):
    """
    A workload filler has the methods for filling up missing information of a workload
    """

    def __init__(self, fillin_config, functions, workloads):

        self.fillin_config = fillin_config
        self.functions = functions
        self.workloads = workloads

    def fill_missing_entries(self):
        """
        Apply default values if specified
        :return:
        """

        default_list = [entry for entry in self.fillin_config if entry['type'] == "fill_missing_entries"]

        for def_config in default_list:
            for wl in self.workloads:
                if wl.tag in def_config['apply_to']:
                    wl.apply_default_metrics(def_config['metrics'], self.functions)

    def match_by_keyword(self):
        """
        Apply a lookup table to add metrics from jobs in a workload to another
        :return:
        """

        match_list = [entry for entry in self.fillin_config if entry['type'] == "match_by_keyword"]

        # Apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0

        for i_match in match_list:
            for wl_source_tag in i_match['source_workloads']:
                wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)
                for wl_dest_tag in i_match['apply_to']:
                    wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                    n_destination_jobs += len(wl_dest.jobs)
                    n_job_matched += wl_dest.apply_lookup_table(wl_source, i_match['similarity_threshold'])

        print_colour("white", "jobs matched/destination jobs = [{}/{}]".format(n_job_matched, n_destination_jobs))

    def recommender_system(self):
        """
        This implements the recommender system corrections
        :return:
        """
        print "TO IMPLEMENT RECOMMENDER SYSTEM"
