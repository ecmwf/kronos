# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from exceptions_iows import ConfigurationError
from kronos_tools.print_colour import print_colour


class WorkloadFiller(object):
    """
    A workload filler has the methods for filling up missing information of a workload
    """

    required_config_fields = [
        'operations',
    ]

    def __init__(self, fillin_config, workloads):

        self.fillin_config = fillin_config
        self.workloads = workloads

        # check that all the required fields are set
        for req_item in self.required_config_fields:
            if req_item not in self.fillin_config.keys():
                raise ConfigurationError("{} requires to specify {}".format(self.__class__.__name__, req_item))

    def fill_missing_entries(self):
        """
        Apply default values if specified
        :return:
        """

        # fields required by this function
        required_function_fields = [
            'type',
            'apply_to',
            'priority',
            'metrics'
        ]

        # pick up all the entries in the function list
        fill_missing_configs = [entry for entry in self.fillin_config['operations'] if entry['type'] == "fill_missing_entries"]

        for miss_config in fill_missing_configs:

            # check config keys
            for req_item in required_function_fields:
                if req_item not in miss_config.keys():
                    raise ConfigurationError("'fill_missing_entries' requires to specify {}".format(req_item))

            # apply function to all the specified workloads
            for wl in self.workloads:
                if wl.tag in miss_config['apply_to']:
                    wl.apply_default_metrics(miss_config, self.fillin_config.get('user_functions'))

    def match_by_keyword(self):
        """
        Apply a lookup table to add metrics from jobs in a workload to another
        :return:
        """

        # fields required by this function
        required_function_fields = [
            'type',
            'priority',
            'keywords',
            'similarity_threshold',
            'source_workloads',
            'apply_to'
        ]

        match_list = [entry for entry in self.fillin_config['operations'] if entry['type'] == "match_by_keyword"]

        # Apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0

        # loop over all the match by keyword entries (there could be more than one..)
        for match_config in match_list:

            # check config keys
            for req_item in required_function_fields:
                if req_item not in match_config.keys():
                    raise ConfigurationError("'match_by_keyword' requires to specify {}".format(req_item))

            for wl_source_tag in match_config['source_workloads']:

                try:
                    wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)
                except StopIteration:
                    raise ConfigurationError("Source Workload {} not found".format(wl_source_tag))

                for wl_dest_tag in match_config['apply_to']:

                    try:
                        wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                    except StopIteration:
                        raise ConfigurationError("Destination Workload {} not found".format(wl_dest_tag))

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

        required_function_fields = [
            'type',
            'priority',
            'n_bins',
            'apply_to',
        ]

        recomm_config_list = [entry for entry in self.fillin_config['operations'] if entry['type'] == "recommender_system"]

        # the recommender system technique is applied to each workload of the list individually..
        for rs_config in recomm_config_list:

            # check config keys
            for req_item in required_function_fields:
                if req_item not in rs_config.keys():
                    raise ConfigurationError("'recommender_system' requires to specify {}".format(req_item))

            for wl_name in rs_config['apply_to']:
                print_colour("green", "Applying recommender system on workload: {}".format(wl_name))
                wl_dest = next(wl for wl in self.workloads if wl.tag == wl_name)
                wl_dest.apply_recommender_system(rs_config)

