#!/usr/bin/env python
from datetime import datetime

from kronos.executor.event_dispatcher import EventDispatcher
from kronos.executor.event_manager import EventManager
from kronos.executor.executor import Executor


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
        time_0 = datetime.now()

        # init the dispatcher
        hdl = EventDispatcher(server_host=self.notification_host, server_port=self.notification_port)
        ev_manager = EventManager()

        # the simulation info
        _submitted_jobs = []
        _finished_jobs = []

        # Main loop over dependencies
        i_submission_cycle = 0
        while not all(j.id in _finished_jobs for j in jobs):

            # Add a timer event every N cycles (just in case..)
            if not i_submission_cycle % self.time_event_cycles:
                ev_manager.add_time_event((datetime.now() - time_0).total_seconds())

            # submit jobs (with several workers..)
            self.submit_eligible_jobs(jobs, _finished_jobs, _submitted_jobs)

            # wait until next message has arrived from the dispatcher
            ev_manager.update_events(hdl.get_next_message())

            # update list of completed jobs
            _finished_jobs = [e.info["job"] for e in ev_manager.get_events(type_filter="Complete")]

            # update cycle counter
            i_submission_cycle += 1

    def submit_eligible_jobs(self, _jobs, fjobs, sjobs):
        """
        Submit the jobs eligible for submission
        :param _jobs:
        :param fjobs:
        :param sjobs:
        :return:
        """

        for j in _jobs:

            # consider this job only if it has not yet run or submitted
            if (j.id not in fjobs) and (j.id not in sjobs):

                # check if all its parent jobs have finished
                if j.depends == [] or all([p in fjobs for p in j.depends]):

                    # append this job to the list of "already submitted jobs"
                    sjobs.append(j.id)

                    # run the job now (with empty dependencies)..
                    j.run([])
