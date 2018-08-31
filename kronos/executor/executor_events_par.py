#!/usr/bin/env python
import logging
from datetime import datetime

import os
from kronos.executor.job_submitter import JobSubmitter
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

        jobs = self.generate_job_internals()

        # init the event manager
        event_manager = Manager(server_host=self.notification_host,
                                server_port=self.notification_port,
                                sim_token=self.simulation_token)

        # init the job submitter
        job_submitter = JobSubmitter(jobs, event_manager, n_submitters=self.n_submitters)

        # the submission loop info
        completed_jobs = []
        completed_jobs_prev = []
        i_submission_cycle = 0

        time_0 = datetime.now()
        new_events = []
        while not all(j.id in completed_jobs for j in jobs):

            # Add a timer event every N cycles (just in case it's needed..)
            if not i_submission_cycle % self.time_event_cycles:
                event_manager.add_time_event((datetime.now()-time_0).total_seconds())

            # submit jobs
            job_submitter.submit_eligible_jobs(new_events=new_events)

            # Get next message from manager
            if not all(j.id in completed_jobs for j in jobs):
                new_events = event_manager.next_events(batch_size=self.event_batch_size)

            # completed job id's
            completed_jobs = set([e.info["job"] for e in event_manager.get_events(type_filter="Complete")])
            if len(completed_jobs) > len(completed_jobs_prev):
                logger.info("completed_jobs: {}/{}".format(len(completed_jobs), len(jobs)))
                completed_jobs_prev = completed_jobs

            # update cycle counter
            i_submission_cycle += 1

        # Finally stop the event dispatcher
        logger.info("Total #events received: {}".format(event_manager.get_total_n_events()))

        # print TOTAL TIMED simulation time (= T_end_last_timed_job - T_start_first_timed_job)
        if job_submitter.initial_submission_time:

            # All arrived messages (may contain duplicates)
            completed_jobs_events = event_manager.get_events(type_filter="Complete")

            jobid_to_timed_flag = {j.id: j.is_job_timed for j in jobs}
            timed_timestamps = [e.info.get("timestamp") for e in completed_jobs_events
                                if jobid_to_timed_flag[e.info("job")]]

            if timed_timestamps:
                last_timed_msg_timestamp = max(timed_timestamps)
                timed_simulation_time = last_timed_msg_timestamp - job_submitter.initial_submission_time
                logger.info("SIMULATION TIME: {}".format(timed_simulation_time))
            else:
                logger.info("INFO: simulation time not available (no timed messaged found..)")

        else:
            logger.info("No timed jobs found.")

    def unsetup(self):
        """
        Various after-run tasks
        :return:
        """

        # Copy the log file into the output directory
        if os.path.exists(os.path.join(os.getcwd(), "kronos-executor.log")):
            logger.info("kronos simulation completed.")
            logger.info("copying {} into {}".format(os.path.join(os.getcwd(), "kronos-executor.log"), self.job_dir))
            copy2(os.path.join(os.getcwd(), "kronos-executor.log"), self.job_dir)
