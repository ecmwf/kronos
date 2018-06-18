#!/usr/bin/env python
# from datetime import datetime
from kronos.executor.job_submitter import JobSubmitter
from kronos.executor.kronos_events.manager import Manager
from kronos.executor.executor import Executor


class ExecutorDepsEventsMultiProc(Executor):
    """
    An ExecutorDepsScheduler passes a schedule of jobs to the real scheduler to be executed.

    """

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorDepsEventsMultiProc, self).__init__(config, schedule, kschedule_file=kschedule_file)

        self.event_batch_size = config.get("event_batch_size", 1)
        self.n_submitters = config.get("n_submitters", 1)

        print "======= Executor multiproc config: ======="
        print "event_batch_size: {}".format(self.event_batch_size)
        print "n_submitters: {}".format(self.n_submitters)

    def run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        jobs = self.generate_job_internals()

        # init the event manager
        event_manager = Manager(server_host=self.notification_host, server_port=self.notification_port)

        # init the job submitter
        job_submitter = JobSubmitter(jobs, event_manager, n_submitters=self.n_submitters)

        # the submission loop info
        completed_jobs = []
        i_submission_cycle = 0

        # time_0 = datetime.now()
        new_events = []
        while not all(j.id in completed_jobs for j in jobs):

            # # Add a timer event every N cycles (just in case it's needed..)
            # if not i_submission_cycle % self.time_event_cycles:
            #     event_manager.add_time_event((datetime.now()-time_0).total_seconds())

            # submit jobs
            # print "1) searching for eligible jobs.."
            job_submitter.submit_eligible_jobs(new_events=new_events)

            # Get next message from manager
            # print "2) getting batch of events.."
            if not all(j.id in completed_jobs for j in jobs):
                new_events = event_manager.next_events(batch_size=self.event_batch_size)

            # completed job id's
            completed_jobs = [e.info["job"] for e in event_manager.get_events(type_filter="Complete")]
            print "completed_jobs: {}/{}".format(len(completed_jobs), len(jobs))

            # update cycle counter
            i_submission_cycle += 1

        # Finally stop the event dispatcher
        event_manager.stop_dispatcher()
