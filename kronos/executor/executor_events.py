#!/usr/bin/env python
from datetime import datetime

from kronos.executor.event_dispatcher import EventDispatcher
from kronos.executor.executor import Executor
from kronos.executor.kronos_event import KronosEvent


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

        # the simulation info
        _simulation_events = []
        _submitted_jobs = []
        _finished_jobs = []

        # Main loop over dependencies
        i_submission_cycle = 0
        while not all(j.id in _finished_jobs for j in jobs):

            # create a timer event every N cycles (just in case is needed)
            if not i_submission_cycle % self.time_event_cycles:
                self.add_timer_event(time_0, _simulation_events)

            # submit jobs (with several workers..)
            self.submit_eligible_jobs(jobs, _finished_jobs, _submitted_jobs)

            # advance the cycle as soon as there is some new event(s)
            hdl.handle_incoming_messages()
            _simulation_events = hdl.events

            # update list of completed jobs
            _finished_jobs = [e.info["job"] for e in _simulation_events if e.type == "Complete"]

            # print "_simulation_events ", [e.event for e in _simulation_events]
            # print "_finished_jobs ", _finished_jobs

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

    def add_timer_event(self, t0, _events):
        """
        Add a time event
        :param t0:
        :param _events:
        :return:
        """

        _current_time = datetime.now()
        _time_from_start = (_current_time - t0).total_seconds()
        _events.append(KronosEvent.from_time(_time_from_start))
