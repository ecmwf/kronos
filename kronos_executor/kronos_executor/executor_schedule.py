#!/usr/bin/env python
from kronos_executor.executor import Executor


class ExecutorDepsScheduler(Executor):
    """
    An ExecutorDepsScheduler passes a schedule of jobs to the real scheduler to be executed.

    Certain elements of the ExecutorDepsScheduler can be overridden by the user.
    """

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorDepsScheduler, self).__init__(config, schedule, kschedule_file=kschedule_file)

    def run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        jobs = self.generate_job_internals()

        # Work through the list of jobs. Launch the first job that is not blocked by any other
        # job.
        # n.b. job.run does not just simply return the ID, as it may be asynchronous. The job
        # handler is responsible for calling back set_job_submitted
        while len(jobs) != 0:
            nqueueing = self.thread_manager.num_running

            found_job = None
            depend_ids = None
            for j in jobs:

                try:
                    depends = j.depends
                    depend_ids = [self._submitted_jobs[d] for d in depends]

                    # We have found a job. Break out of the search
                    found_job = j
                    break

                except KeyError:
                    # Go on to the next job in the list
                    pass

            if found_job:
                found_job.run(depend_ids)
                jobs.remove(found_job)

            else:
                # If there are no unblocked jobs, but there are still jobs in the submit queue,
                # wait until something happens
                self.thread_manager.wait_until(nqueueing-1)

        # Wait until we are done
        self.thread_manager.wait()
