from exceptions_iows import ConfigurationError
from jobs import ModelJob
from kronos_tools.print_colour import print_colour
import numpy as np

from synthetic_app import SyntheticApp
from time_signal import TimeSignal, time_signal_names


class SyntheticWorkloadGenerator(object):
    """
    This class represents a generator thaat reads the output of the clusters and
    produces a synthetic workload according to matching strategies:
    -  random: jobs are randomly generated from the clusters to match the submit rate
    - match probability: jobs are generated to match PDF of jobs..
    """
    required_config_fields = [
                             "type"
                             "random_seed",
                             "tuning_factors",
                             "submit_rate_factor",
                             "synthapp_n_proc",
                             "synthapp_n_nodes",
                             "synthapp_n_frames",
                             "total_submit_interval",
                             ]

    def __init__(self, config_generator, clusters):
        self.config_generator = config_generator
        self.clusters = clusters

    def check_config(self):
        """
        Check the keys of the configuration
        :return:
        """

        for req_item in self.required_config_fields:
            if req_item not in self.config_generator.keys():
                raise ConfigurationError("{} requires to specify {}".format(self.__class__.__name__, req_item))
            setattr(self, req_item, self.config_generator[req_item])

    def generate_synthetic_apps(self):
        """
        Main method that call the specific generation method requested
        :return:
        """

        generation_func = getattr(self, self.config_generator['type'], None)

        if generation_func:
            return generation_func()
        else:
            raise ConfigurationError("'type' in generation configuration not recognized")

    def match_job_pdf(self):
        """
        Generate jobs matching PDF of jobs in the time window of the workload
        :return:
        """
        # raise NotImplementedError("not implemented!")

        generated_sa_from_all_wl = []

        # generate a synthetic workload for each cluster of jobs
        for cluster in self.clusters:

            # calculate the submit rate from the selected workload
            start_times = [j.time_start for j in cluster['jobs_for_clustering']]
            real_submit_rate = float(len(start_times)) / (max(start_times) - min(start_times))
            requested_submit_rate = real_submit_rate * self.config_generator['submit_rate_factor']
            n_modelled_jobs = int(requested_submit_rate * self.config_generator['total_submit_interval'])

            if not n_modelled_jobs:
                out_str = 'Low submit rate! '
                out_str += 'real={} jobs/sec '.format(real_submit_rate)
                out_str += 'requested={} jobs/sec '.format(requested_submit_rate)
                out_str += 'n of jobs {} '.format(n_modelled_jobs)
                out_str += '=> Number of jobs will be set to *1*'
                print_colour("orange", out_str)
                n_modelled_jobs = 1

            # create random vector of cluster indices and start times consistent with the PDF of the real start times
            np.random.seed(self.config_generator['random_seed'])
            vec_clust_indexes = np.random.randint(cluster['cluster_matrix'].shape[0], size=n_modelled_jobs)

            # find the PDF of jobs start times
            bins = np.linspace(0, self.config_generator['total_submit_interval'], 50)

            # normalize the vector of start time to make it in [0, max T submit]
            start_times_vec = np.asarray(start_times)
            start_times_norm = (start_times_vec-min(start_times_vec))/(max(start_times_vec)-min(start_times_vec))
            start_times_norm *= self.config_generator['total_submit_interval']

            # then calculate the PDF
            time_start_pdf, time_start_bin_edges = np.histogram(start_times_norm, bins, density=False)
            time_start_pdf_norm = time_start_pdf/float(sum(time_start_pdf))
            time_start_bin_mid = (time_start_bin_edges[:-1]+time_start_bin_edges[1:])/2.0

            # then calculate a random distribution of time start from the provided PDF
            start_times_vec_sa = np.random.choice(time_start_bin_mid, p=time_start_pdf_norm, size=n_modelled_jobs)

            # loop over the clusters and generates jos as needed
            generated_model_jobs = []
            for cc, idx in enumerate(vec_clust_indexes):

                ts_dict = {}
                row = cluster['cluster_matrix'][idx, :]
                ts_yvalues = np.split(row, len(time_signal_names))
                for tt, ts_vv in enumerate(ts_yvalues):
                    ts_name = time_signal_names[tt]
                    ts = TimeSignal(ts_name).from_values(ts_name, np.arange(len(ts_vv)), ts_vv)
                    ts_dict[ts_name] = ts

                job = ModelJob(
                    time_start=start_times_vec_sa[cc],
                    duration=None,
                    ncpus=self.config_generator['synthapp_n_cpu'],
                    nnodes=self.config_generator['synthapp_n_nodes'],
                    timesignals=ts_dict,
                    label="job-{}".format(cc)
                )
                generated_model_jobs.append(job)

            # --- then create the synthetic apps from the generated model jobs --
            modelled_sa_jobs = self.model_jobs_to_sa(generated_model_jobs, cluster['source-workload'])

            generated_sa_from_all_wl.extend(modelled_sa_jobs)

        return generated_sa_from_all_wl

    def match_job_pdf_exact(self):
        """
        Generate jobs matching PDF of jobs in the time window of the workload (trying an exact distribution..)
        :return:
        """

        generated_sa_from_all_wl = []

        # generate a synthetic workload for each cluster of jobs
        for cluster in self.clusters:

            # calculate the submit rate from the selected workload
            start_times = [j.time_start for j in cluster['jobs_for_clustering']]
            # real_submit_rate = float(len(start_times)) / (max(start_times) - min(start_times))

            # find the PDF of jobs start times
            sa_bins = np.linspace(0, self.config_generator['total_submit_interval'], 15)

            # normalize the vector of start time to make it in [0, max T submit]
            start_times_vec = np.asarray(start_times)
            start_times_norm = (start_times_vec - min(start_times_vec)) / (max(start_times_vec) - min(start_times_vec))
            start_times_norm *= self.config_generator['total_submit_interval']

            # then calculate the PDF
            time_start_pdf, time_start_bin_edges = np.histogram(start_times_norm, sa_bins, density=False)

            # then calculate an "exact" distribution of time start from the provided PDF
            sa_time_ratio = self.config_generator['submit_rate_factor']
            start_times_vec_sa = np.asarray([])
            for bb in range(len(time_start_pdf)):
                y_min = sa_bins[bb]
                y_max = sa_bins[bb + 1]
                n_sa_bin = int(time_start_pdf[bb] * sa_time_ratio)
                random_y_values = y_min + np.random.rand(n_sa_bin) * (y_max-y_min)
                start_times_vec_sa = np.append(start_times_vec_sa, random_y_values)

            # create random vector of cluster indices and start times consistent with the PDF of the real start times
            n_modelled_jobs = len(start_times_vec_sa)
            np.random.seed(self.config_generator['random_seed'])
            vec_clust_indexes = np.random.randint(cluster['cluster_matrix'].shape[0], size=n_modelled_jobs)

            # loop over the clusters and generates jos as needed
            generated_model_jobs = []
            for cc, idx in enumerate(vec_clust_indexes):

                ts_dict = {}
                row = cluster['cluster_matrix'][idx, :]
                ts_yvalues = np.split(row, len(time_signal_names))
                for tt, ts_vv in enumerate(ts_yvalues):
                    ts_name = time_signal_names[tt]
                    ts = TimeSignal(ts_name).from_values(ts_name, np.arange(len(ts_vv)), ts_vv)
                    ts_dict[ts_name] = ts

                job = ModelJob(
                    time_start=start_times_vec_sa[cc],
                    duration=None,
                    ncpus=self.config_generator['synthapp_n_cpu'],
                    nnodes=self.config_generator['synthapp_n_nodes'],
                    timesignals=ts_dict,
                    label="job-{}".format(cc)
                )
                generated_model_jobs.append(job)

            # --- then create the synthetic apps from the generated model jobs --
            modelled_sa_jobs = self.model_jobs_to_sa(generated_model_jobs, cluster['source-workload'])

            generated_sa_from_all_wl.extend(modelled_sa_jobs)

        return generated_sa_from_all_wl


    def match_job_rate(self):
        """
        Generates model jobs to match scaled submit rate of jobs
        :return:
        """

        generated_sa_from_all_wl = []

        # generate a synthetic workload for each cluster of jobs
        for cluster in self.clusters:

            # calculate the submit rate from the selected workload..
            start_times = [j.time_start for j in cluster['jobs_for_clustering']]
            real_submit_rate = float(len(start_times)) / (max(start_times) - min(start_times))
            requested_submit_rate = real_submit_rate * self.config_generator['submit_rate_factor']
            n_modelled_jobs = int(requested_submit_rate * self.config_generator['total_submit_interval'])

            if not n_modelled_jobs:
                out_str = 'Low submit rate! '
                out_str += 'real={} jobs/sec '.format(real_submit_rate)
                out_str += 'requested={} jobs/sec '.format(requested_submit_rate)
                out_str += 'n of jobs {} '.format(n_modelled_jobs)
                out_str += '=> Nunber of jobs will be set to *1*'
                print_colour("orange", out_str)
                n_modelled_jobs = 1

            # create a random vector of start times and a random vector of cluster indices
            np.random.seed(self.config_generator['random_seed'])
            start_times_vec = np.random.rand(n_modelled_jobs) * self.config_generator['total_submit_interval']
            vec_clust_indexes = np.random.randint(cluster['cluster_matrix'].shape[0], size=n_modelled_jobs)

            # loop over the clusters and generates jos as needed
            generated_model_jobs = []
            for cc, idx in enumerate(vec_clust_indexes):

                ts_dict = {}
                row = cluster['cluster_matrix'][idx, :]
                ts_yvalues = np.split(row, len(time_signal_names))
                for tt, ts_vv in enumerate(ts_yvalues):
                    ts_name = time_signal_names[tt]
                    ts = TimeSignal(ts_name).from_values(ts_name, np.arange(len(ts_vv)), ts_vv)
                    ts_dict[ts_name] = ts

                job = ModelJob(
                    time_start=start_times_vec[cc],
                    duration=None,
                    ncpus=self.config_generator['synthapp_n_cpu'],
                    nnodes=self.config_generator['synthapp_n_nodes'],
                    timesignals=ts_dict,
                    label="job-{}".format(cc)
                )
                generated_model_jobs.append(job)

            # --- then create the synthetic apps from the generated model jobs --
            modelled_sa_jobs = self.model_jobs_to_sa(generated_model_jobs, cluster['source-workload'])

            generated_sa_from_all_wl.extend(modelled_sa_jobs)

        return generated_sa_from_all_wl

    @staticmethod
    def model_jobs_to_sa(model_jobs, label):
        """
        This method takes a list of model jobs and translates tham into synthetic apps
        :param model_jobs:
        :param label:
        :return:
        """

        sa_list = []
        for cc, job in enumerate(model_jobs):
            app = SyntheticApp(
                job_name="RS-appID-{}".format(cc),
                time_signals=job.timesignals,
                ncpus=job.ncpus,
                nnodes=job.nnodes,
                time_start=job.time_start,
                label=label
            )

            sa_list.append(app)

        return sa_list
