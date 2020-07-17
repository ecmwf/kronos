#!/usr/bin/env python
from kronos_executor.executor import Executor


class ExecutorDepsScheduler(Executor):
    """
    An ExecutorDepsScheduler passes a time_schedule of jobs to the real scheduler to be executed.

    Certain elements of the ExecutorDepsScheduler can be overridden by the user.
    """

    def __init__(self, config, schedule, arg_config=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorDepsScheduler, self).__init__(config, schedule, arg_config=arg_config)

    def do_run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        while len(self.jobs) != 0:
            submittable = []
            job_deps = []
            for j in self.jobs:

                try:
                    depends = j.depends
                    depend_ids = [self.submitted_job_ids[d] for d in depends]

                    # We have found a job
                    submittable.append(j)
                    job_deps.append(depend_ids)

                except KeyError:
                    # Go on to the next job in the list
                    pass

            self.job_submitter.submit(submittable, job_deps)
            for job in submittable:
                self.jobs.remove(job)
