import os

import data_analysis
import workload_data
import generator
from exceptions_iows import ConfigurationError

from synthetic_app import SyntheticWorkload
from kronos_tools.print_colour import print_colour
from config.config import Config
from workload_fill_in import WorkloadFiller


class KronosModel(object):
    """
    The model class operates on a list of workloads:
    1) applies corrections to the workloads according to the configuration instructions passed by the user
    2) applies a modelling strategy
    3) returns a synthetic workload
    """

    def __init__(self, workloads, config):

        assert all(isinstance(wl, workload_data.WorkloadData) for wl in workloads)
        assert isinstance(config, Config)

        # configuration parameters
        self.config = config
        self.config_fill_in = None
        self.config_classification = None
        self.config_generator = None
        self.config_functions = None

        self.workloads = workloads
        self.jobs_for_clustering = []
        self.clusters = []
        self.modelled_sa_jobs = []

        # check all the configurations
        for k, v in self.config.model.iteritems():
            if not hasattr(self, 'config_'+k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, 'config_'+k, v)

    def generate_model(self):
        """
        takes the list of workloads and performs three actions in sequence:
        1) applies user instructions on potentially partially filled workloads
        2) generate a workload model through clustering+generation process
        3) export the workload as a synthetic workload
        :return:
        """

        # 1) apply fill-in strategies
        self._apply_workload_fillin()

        # 2) apply classification
        self._apply_classification()

        # 3) apply generation
        self._apply_generation()

    def _apply_workload_fillin(self):
        """
        Apply user requested strategies to the workloads
        :return:
        """

        filler = WorkloadFiller(self.config_fill_in, self.config_functions, self.workloads)

        # call functions corresponding to fill_in types
        for fillin_function in self.config_fill_in:
            getattr(filler, fillin_function['type'])()

    def export_synthetic_workload(self):
        """
        Export the synthetic workload generated from the model
        :return:
        """

        if not self.modelled_sa_jobs:
            raise ConfigurationError("cannot export before generating the model!")

        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)
        sa_workload.set_tuning_factors(self.config_generator['tuning_factors'])

        # export the synthetic workload
        ksf_path = os.path.join(self.config.dir_output, self.config.ksf_filename)
        sa_workload.export_ksf(ksf_path, self.config_generator['sa_n_frames'])

    def _check_jobs(self):
        """
        Check that all jobs have the minimum required fields..
        :return:
        """
        print_colour("green", "Checking all jobs before classification..")

        # check only the jobs to be used for classification..
        for wl_name in self.config_classification['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_name)
            wl.check_jobs()

    def _apply_classification(self):
        """
        Apply modelling classification to selected workloads
        :return:
        """

        # Split the workloads if required..
        if self.config_classification.get('split_workload', None):

            split_config = self.config_classification['split_workload']
            split_config_output = split_config['output_workloads']

            wl = next(wl for wl in self.workloads if wl.tag == split_config['apply_to'])

            sub_workloads = wl.split_by_keywords(split_config_output)

            for sub_wl in sub_workloads:
                print_colour("cyan", "splitting has created workload {} with {} jobs".format(sub_wl.tag, len(sub_wl.jobs)))

            # extend the internal workloads with the result of the split operation
            self.workloads.extend(sub_workloads)

        # check validity of jobs before doing the actual modelling..
        # NB: the preliminary phase of workload manipulation (defaults, lookup tables and recommender sys
        # should have produced a set of "complete" and therefore valid jobs)
        self._check_jobs()

        # loop over all the workloads used to create the synthetic workload
        for wl_entry in self.config_classification['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_entry)

            print_colour("green", "-------> applying classification to workload {}".format(wl_entry))

            # Apply clustering
            classific_config = self.config_classification['clustering']
            cluster_handler = data_analysis.factory(classific_config['type'], classific_config)
            cluster_handler.cluster_jobs(wl.jobs_to_matrix(classific_config['n_ts_bins']))
            clusters_matrix = cluster_handler.clusters
            clusters_labels = cluster_handler.labels

            self.clusters.append({
                                  'source-workload': wl_entry,
                                  'jobs_for_clustering': wl.jobs,
                                  'cluster_matrix': clusters_matrix,
                                  'labels': clusters_labels,
                                  })

    def _apply_generation(self):
        """
        Generate synthetic workload form the supplied model jobs
        :return:
        """

        sapps_generator = generator.SyntheticWorkloadGenerator(self.config_generator, self.clusters)
        self.modelled_sa_jobs = sapps_generator.generate_synthetic_apps()
