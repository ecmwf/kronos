#!/usr/bin/env python
from datetime import datetime

from kronos.executor.kronos_events.manager import Manager
from kronos.executor.executor import Executor
from kronos.executor.kronos_events.dispatcher import EventDispatcher


class ExecutorDepsEvents(Executor):
    """
    An ExecutorDepsScheduler passes a schedule of jobs to the real scheduler to be executed.

    """

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorDepsEvents, self).__init__(config, schedule, kschedule_file=kschedule_file)

    def run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        jobs = self.generate_job_internals()

        # init the event dispatcher and manager
        ev_dispatcher = EventDispatcher(server_host=self.notification_host,
                                        server_port=self.notification_port)

        # init the event manager
        event_manager = Manager(ev_dispatcher)

        # the submission loop info
        submitted_jobs = []
        completed_job_ids = []
        i_submission_cycle = 0

        time_0 = datetime.now()
        while not all(j.id in completed_job_ids for j in jobs):

            # Add a timer event every N cycles (just in case..)
            if not i_submission_cycle % self.time_event_cycles:
                event_manager.add_time_event((datetime.now()-time_0).total_seconds())

            # submit jobs (with several workers..)
            self.submit_eligible_jobs(jobs, event_manager, submitted_jobs)

            # wait until next message has arrived from the dispatcher
            event_manager.wait_for_new_event()

            # get updated list of completed jobs
            completed_job_ids = [e.info["job"] for e in event_manager.get_events(type_filter="Complete")]

            # update cycle counter
            i_submission_cycle += 1

    def submit_eligible_jobs(self, _jobs, ev_manager, sub_jobs):
        """
        Submit the jobs eligible for submission
        :param _jobs:
        :param compl_jobs:
        :param sub_jobs:
        :return:
        """

        compl_jobs = [e.info["job"] for e in ev_manager.get_events(type_filter="Complete")]

        for j in _jobs:

            # consider this job only if it has not yet run or submitted
            if (j.id not in compl_jobs) and (j.id not in sub_jobs):

                # check if all its parent jobs have finished
                if j.depends == [] or ev_manager.are_job_dependencies_fullfilled(j):

                    # append this job to the list of "already submitted jobs"
                    sub_jobs.append(j.id)

                    # run the job now (with empty dependencies)..
                    j.run([])
