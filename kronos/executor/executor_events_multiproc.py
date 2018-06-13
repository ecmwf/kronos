#!/usr/bin/env python
from datetime import datetime
import math

from kronos.executor.job_submitter import JobSubmitter
from kronos.executor.kronos_events.manager import Manager
from kronos.executor.executor import Executor
from kronos.executor.kronos_events.dispatcher import EventDispatcher


class ExecutorDepsEventsMultiProc(Executor):
    """
    An ExecutorDepsScheduler passes a schedule of jobs to the real scheduler to be executed.

    """

    event_batch_size = 20
    event_batch_size_proc = 5
    n_submitters = int(math.ceil(event_batch_size / float(event_batch_size_proc)))

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorDepsEventsMultiProc, self).__init__(config, schedule, kschedule_file=kschedule_file)

    def run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        jobs = self.generate_job_internals()

        # init the event dispatcher and manager
        ev_dispatcher = EventDispatcher(server_host=self.notification_host, server_port=self.notification_port)

        # init the event manager
        event_manager = Manager(ev_dispatcher)

        # init the job submitter
        job_submitter = JobSubmitter(jobs, event_manager, n_submitters=5)

        # the submission loop info
        completed_jobs = []
        i_submission_cycle = 0

        time_0 = datetime.now()
        new_events = None
        while not all(j.id in completed_jobs for j in jobs):

            # Add a timer event every N cycles (just in case..)
            if not i_submission_cycle % self.time_event_cycles:
                event_manager.add_time_event((datetime.now()-time_0).total_seconds())

            # submit jobs
            job_submitter.submit_eligible_jobs(new_events=new_events)

            # Get next message from manager
            if not all(j.id in completed_jobs for j in jobs):
                new_events = event_manager.next_events(batch_size=10)
                # new_event = event_manager.next_event()

            # completed job id's
            completed_jobs = [e.info["job"] for e in event_manager.get_events(type_filter="Complete")]

            # print "completed jobs {}".format(completed_jobs)

            # update cycle counter
            i_submission_cycle += 1

        # Finally stop the event dispatcher
        ev_dispatcher.stop()
