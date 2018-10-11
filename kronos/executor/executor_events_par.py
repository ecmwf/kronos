#!/usr/bin/env python
import logging
from datetime import datetime

import os
from kronos.executor.job_submitter import JobSubmitter
from kronos.executor.kronos_events import EventComplete
from kronos.executor.kronos_events.manager import Manager
from kronos.executor.executor import Executor
from shutil import copy2

logger = logging.getLogger(__name__)


class ExecutorEventsPar(Executor):
    """
    An ExecutorDepsScheduler passes a schedule of jobs to the real scheduler to be executed.

    """

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorEventsPar, self).__init__(config, schedule, kschedule_file=kschedule_file)

        self.event_batch_size = config.get("event_batch_size", 10)
        self.n_submitters = config.get("n_submitters", 4)

        self.event_manager = None
        self.job_submitter = None
        self.jobs = None

        logger.info("======= Executor multiproc config: =======")
        logger.info("events notification host: {}".format(self.notification_host))
        logger.info("events notification port: {}".format(self.notification_port))
        logger.info("events batch size       : {}".format(self.event_batch_size))
        logger.info("job submitting processes: {}".format(self.n_submitters))

    def run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        self.jobs = self.generate_job_internals()

        # init the event manager
        self.event_manager = Manager(server_host=self.notification_host,
                                     server_port=self.notification_port,
                                     sim_token=self.simulation_token)

        # init the job submitter
        self.job_submitter = JobSubmitter(self.jobs, self.event_manager, n_submitters=self.n_submitters)

        # the submission loop info
        completed_jobs = []
        completed_jobs_prev = []
        i_submission_cycle = 0

        time_0 = datetime.now()
        new_events = []
        while not all(j.id in completed_jobs for j in self.jobs):

            # NOTE: To be used with great care as number/size of time message could be huge..
            # # Add a timer event every N cycles (just in case it's needed..)
            # if not i_submission_cycle % self.time_event_cycles:
            #     self.event_manager.add_time_event((datetime.now()-time_0).total_seconds())

            # submit jobs
            self.job_submitter.submit_eligible_jobs(new_events=new_events)

            # Get next message from manager
            if not all(j.id in completed_jobs for j in self.jobs):
                new_events = self.event_manager.next_events(batch_size=self.event_batch_size)

            # completed job id's
            completed_jobs = set([e.info["job"] for e in self.event_manager.get_events(type_filter="Complete")])
            if len(completed_jobs) > len(completed_jobs_prev):
                logger.info("completed_jobs: {}/{}".format(len(completed_jobs), len(self.jobs)))
                completed_jobs_prev = completed_jobs

            # update cycle counter
            i_submission_cycle += 1

        # Finally stop the event dispatcher
        logger.info("Total #events received: {}".format(self.event_manager.get_total_n_events()))

    def unsetup(self):
        """
        Various after-run tasks
        :return:
        """

        # print TOTAL TIMED simulation time (= T_end_last_timed_job - T_start_first_timed_job)
        if self.job_submitter.initial_submission_time:

            jobid_to_timed_flag = {j.id: j.is_job_timed for j in self.jobs}
            completed_jobs_and_timings = [(ev, time) for (ev, time) in self.event_manager.get_timed_events()
                                          if isinstance(ev, EventComplete)]

            times_of_timed_jobs = [time for (ev, time) in completed_jobs_and_timings
                                   if jobid_to_timed_flag[ev.info["job"]]]

            if times_of_timed_jobs:
                last_timed_msg_timestamp = max(times_of_timed_jobs)
                timed_simulation_time = last_timed_msg_timestamp - self.job_submitter.initial_submission_time

                logger.info("=" * 37)
                logger.info("SIMULATION TIME: {:20.2f}".format(timed_simulation_time))
                logger.info("SIMULATION  T_0: {:20.2f}".format(self.job_submitter.initial_submission_time))
                logger.info("SIMULATION  T_1: {:20.2f}".format(last_timed_msg_timestamp))
                logger.info("=" * 37)
            else:
                logger.warning("INFO: simulation time not available (no timed messaged found..)")

        else:
            logger.info("No timed jobs found.".upper())

        # Copy the log file into the output directory
        if os.path.exists(os.path.join(os.getcwd(), "kronos-executor.log")):
            logger.info("kronos simulation completed.".upper())
            logger.info("copying {} into {}".format(os.path.join(os.getcwd(), "kronos-executor.log"), self.job_dir))
            copy2(os.path.join(os.getcwd(), "kronos-executor.log"), self.job_dir)

        # finally terminate the dispatcher process
        self.event_manager.stop_dispatcher()
