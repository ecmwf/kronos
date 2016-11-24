import json
import sys

from jobs import ModelJob
from kronos_tools import print_colour
from time_signal import TimeSignal
from workload_data import WorkloadData


class KPFFileHandler(object):
    """
    Class that encapsulates some of the operations for a kpf file
    """
    def __init__(self):
        pass

    @staticmethod
    def save_kpf(workloads, kpf_filename=None):
        """
        Export a kpf file
        :param workloads: list of workloads
        :param kpf_filename:
        :return:
        """

        assert all(isinstance(wl, WorkloadData) for wl in workloads)
        print_colour.print_colour('white', 'exporting kpf file: {}'.format(kpf_filename))

        if kpf_filename:

            kpf_data = []

            if not kpf_filename.endswith('.kpf'):
                print("extension .ksp will be appended")
                kpf_filename += '.kpf'

            for wl in workloads:

                # collect data from jobs in the dataset
                wl_jobs_json = []
                for job in wl.jobs:

                    job_entry_data = {
                        'time_start': job.time_start,
                        'ncpus': job.ncpus,
                        'nnodes': job.nnodes,
                        'duration': job.duration,
                        'label': job.label,
                        'scheduler_timing': job.scheduler_timing,
                    }

                    # append timeseries signals
                    timesignals_dict = {}
                    for tsk, tsv in job.timesignals.items():
                        if tsv:
                            timesignals_dict[tsk] = {
                                'xvalues': list(tsv.xvalues),
                                'yvalues': list(tsv.yvalues)
                            }

                    job_entry_data['timesignals'] = timesignals_dict

                    # append this job to the dataset job list
                    wl_jobs_json.append(job_entry_data)

                kpf_workload_entry = {
                    'tag': wl.tag,
                    'jobs': wl_jobs_json,
                }

                # for each job write the relevant data
                kpf_data.append(kpf_workload_entry)

            # export kpf file
            with open(kpf_filename, 'w') as f:
                json.dump(kpf_data, f, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            print "filename not specified!"
            sys.exit(-1)

    @staticmethod
    def load_kpf(kpf_filename):
        """
        Load a kpf file and returns a workload
        :param kpf_filename:
        :return: jobs
        """

        # list of job sets to return
        workloads = []

        with open(kpf_filename, 'r') as f:
            workloads_json = json.load(f)

        for wl_json in workloads_json:
            jobs = wl_json['jobs']
            model_jobs = []

            for job in jobs:

                # create dictionary of timesignals
                ts_dict = {}
                for tsk, tsv in job['timesignals'].items():
                    ts_dict[tsk] = TimeSignal(tsk).from_values(tsk,
                                                               tsv['xvalues'],
                                                               tsv['yvalues'],
                                                               )

                model_jobs.append(
                                    ModelJob(
                                            time_start=job['time_start'],
                                            ncpus=job['ncpus'],
                                            nnodes=job['nnodes'],
                                            duration=job['duration'],
                                            label=job['label'],
                                            scheduler_timing=job['scheduler_timing'],
                                            time_series=ts_dict,
                                            )
                                 )

            workloads.append(WorkloadData(jobs=model_jobs, tag=wl_json['tag']))

        # return the workload
        return workloads


