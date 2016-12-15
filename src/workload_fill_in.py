from kronos_tools.print_colour import print_colour


class WorkloadFiller(object):
    """
    A workload filler has the methods for filling up missing information of a workload
    """

    def __init__(self, fillin_config, workloads):

        self.fillin_config = fillin_config
        self.workloads = workloads

    def fill_missing_entries(self):
        """
        Apply default values if specified
        :return:
        """

        fill_missing_configs = [entry for entry in self.fillin_config['operations'] if entry['type'] == "fill_missing_entries"]

        for miss_config in fill_missing_configs:
            for wl in self.workloads:
                if wl.tag in miss_config['apply_to']:
                    wl.apply_default_metrics(miss_config, self.fillin_config.get('user_functions'))

    def match_by_keyword(self):
        """
        Apply a lookup table to add metrics from jobs in a workload to another
        :return:
        """

        match_list = [entry for entry in self.fillin_config['operations'] if entry['type'] == "match_by_keyword"]

        # Apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0

        # loop over all the match by keyword entries (there could be more than one..)
        for match_config in match_list:
            for wl_source_tag in match_config['source_workloads']:

                wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)

                for wl_dest_tag in match_config['apply_to']:

                    wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                    n_destination_jobs += len(wl_dest.jobs)

                    n_job_matched += wl_dest.apply_lookup_table(wl_source,
                                                                match_config['similarity_threshold'],
                                                                match_config['priority'],
                                                                match_config['keywords'],
                                                                )

        print_colour("white", "jobs matched/destination jobs = [{}/{}]".format(n_job_matched, n_destination_jobs))

    def recommender_system(self):
        """
        This implements the recommender system corrections
        :return:
        """

        recomm_config_list = [entry for entry in self.fillin_config['operations'] if entry['type'] == "recommender_system"]

        # the recommender system technique is applied to each workload of the list individually..
        for rs_config in recomm_config_list:

            for wl_name in rs_config['apply_to']:
                print_colour("green", "Applying recommender system on workload: {}".format(wl_name))
                wl_dest = next(wl for wl in self.workloads if wl.tag == wl_name)
                wl_dest.apply_recommender_system(rs_config)

