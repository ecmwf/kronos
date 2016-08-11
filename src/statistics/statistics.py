import matplotlib.pyplot as plt
import collections
import numpy as np
import csv

from logreader.scheduler_reader import PBSDataSet, AccountingDataSet
from tools.print_colour import print_colour


class Statistics(object):

    """ statistics class for a dataset """

    def __init__(self, dataset):

        self.dataset = dataset
        self.summary_data = {}

    def calculate_statistics(self):

        if isinstance(self.dataset, AccountingDataSet) or isinstance(self.dataset, PBSDataSet):

            list_jobs_op = [i_job for i_job in self.dataset.joblist if i_job.queue_type in ['op', 'of']]
            cpus_hours_vec = np.asarray([i_job.ncpus*(i_job.time_end-i_job.time_start)/3600. for i_job in list_jobs_op])

            queue_list = list(set(i_job.queue_type for i_job in self.dataset.joblist))
            queue_time_vec = np.asarray(i_job.time_in_queue for i_job in self.dataset.joblist)

            run_time_vec = np.asarray([i_job.runtime for i_job in list_jobs_op])

            for iq in queue_list:

                list_jobs_in_queue = [i_job for i_job in self.dataset.joblist if i_job.queue_type == iq]

                n_jobs = len(list_jobs_in_queue)
                mean_runtime = sum([i_job.runtime for i_job in list_jobs_in_queue])/float(n_jobs)
                mean_queue_time = sum([i_job.time_in_queue for i_job in list_jobs_in_queue])/float(n_jobs)
                mean_cpu_hours = sum([i_job.ncpus*i_job.runtime/3600. for i_job in list_jobs_in_queue])/float(n_jobs)

                if n_jobs >= 1:
                    self.summary_data[iq] = collections.OrderedDict([('N jobs', '{:d}'.format(n_jobs)),
                                                                    ('mean runtime [s]', '{:.0f}'.format(mean_runtime)),
                                                                    ('mean queue time [s]', '{:.0f}'.format(mean_queue_time)),
                                                                    ('mean cpu hours', '{:.3f}'.format(mean_cpu_hours))
                                                                    ])

        else:

            print_colour("orange", "plotter for {} dataset not implemented!".format(type(self.dataset)))

    def export_csv(self, run_tag, out_dir):

        queue_list = self.summary_data.keys()
        params_names = self.summary_data[queue_list[0]].keys()

        with open(out_dir + "/" + run_tag + '_statistics_summary.csv', 'w') as f:
            w = csv.writer(f)
            w.writerow(['queue'] + params_names)

            for iq in self.summary_data.keys():
                w.writerow([iq]+self.summary_data[iq].values())
