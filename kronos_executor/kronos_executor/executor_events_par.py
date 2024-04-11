#!/usr/bin/env python

import logging
from datetime import datetime

from kronos_executor.executor import Executor
from kronos_executor.kronos_events import EventComplete, EventFailed
from kronos_executor.kronos_events.manager import Manager
from kronos_executor.kronos_events.time_ticker import TimeTicker

logger = logging.getLogger(__name__)


class ExecutorEventsPar(Executor):
    """
    An ExecutorDepsScheduler passes a time_schedule of jobs to the real scheduler to be executed.

    """

    def __init__(self, config, schedule, arg_config=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorEventsPar, self).__init__(config, schedule, arg_config=arg_config)

        self.event_batch_size = config.get("event_batch_size", 10)
        self.n_submitters = config.get("n_submitters", 4)

        self.event_manager = None

        self.submitted_jobs = set()
        self.initial_submission_time = None
        self.deps_to_jobs_tree = None
        self.job_to_deps = None

        logger.info("======= Executor multiproc config: =======")
        logger.info("events notification host: {}".format(self.notification_host))
        logger.info("events notification port: {}".format(self.notification_port))
        logger.info("events batch size       : {}".format(self.event_batch_size))
        logger.info("job submitting processes: {}".format(self.n_submitters))

    def setup(self):
        """
        Some preparation before the simulation
        :return:
        """

        # still need to execute the parent setup
        super(ExecutorEventsPar, self).setup()

        # init the event manager
        self.event_manager = Manager(server_host=self.notification_host,
                                     server_port=self.notification_port,
                                     sim_token=self.simulation_token)

        # generate the dependency tree structure
        self.deps_to_jobs_tree, self.job_to_deps = self.build_deps_to_job_tree()

    def build_deps_to_job_tree(self):
        """
        Build a structure that allows getting jobs that depend on a particular event
        :return:
        """

        # structure dependency->jobs
        _deps_2_jobs = {}
        for j in self.jobs:
            for d in j.depends:
                _deps_2_jobs.setdefault(d.get_hashed(), []).append(j)

        # structure j.id->dependency
        job_2_deps = {}
        for j in self.jobs:
            job_2_deps[j.id] = [d.get_hashed() for d in j.depends]

        return _deps_2_jobs, job_2_deps

    def do_run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        # the submission loop info
        completed_jobs = []
        completed_jobs_prev = []
        i_submission_cycle = 0

        new_events = []
        time_0 = datetime.now()
        time_ticker = TimeTicker(time_0)

        # ========= MAIN SIMULATION LOOP =========
        logger.info("Running..")
        while not all(j.id in completed_jobs for j in self.jobs):

            # Add a time event for every second elapsed since last call
            new_seconds = time_ticker.get_elapsed_seconds(datetime.now())
            for i_sec in new_seconds:
                self.event_manager.add_time_event(i_sec)
                logger.debug("added second {}".format(i_sec))

            # submit jobs
            self.submit_eligible_jobs(new_events=new_events)

            # Get next message from manager
            if not all(j.id in completed_jobs for j in self.jobs):
                new_events = self.event_manager.get_latest_events(batch_size=self.event_batch_size)

                for event in new_events:
                    if isinstance(event, EventFailed):
                        raise Executor.SimulationFailed(
                            "Job {} ({}) failed. Aborting.".format(event.info["job"], event.info["app"]))

            # completed job id's
            completed_jobs = set([e.info["job"]
                for e in self.event_manager.get_events(type_filter="Complete")
                if e.info["job"].count(".") == 0])
            if len(completed_jobs) > len(completed_jobs_prev):
                logger.info("completed_jobs: {}/{}".format(len(completed_jobs), len(self.jobs)))
                completed_jobs_prev = completed_jobs

            # update cycle counter and ref time
            i_submission_cycle += 1

        # Finally stop the event dispatcher
        logger.info("Total #events received: {}".format(self.event_manager.get_total_n_events()))

    def submit_eligible_jobs(self, new_events=None):
        """
        Submit the jobs eligible for submission
        :param new_events:
        :return:
        """

        submittable_jobs = []

        if not new_events:
            submittable_jobs = [j for j in self.jobs if not j.depends and j.id not in self.submitted_jobs]
            self.submitted_jobs.update(j.id for j in submittable_jobs)

        else:
            for new_event in new_events:

                triggered = self.deps_to_jobs_tree.get(new_event.get_hashed(), [])
                for j in triggered:

                    if new_event.get_hashed() in self.job_to_deps[j.id]:
                        self.job_to_deps[j.id].remove(new_event.get_hashed())

                    if not self.job_to_deps[j.id] and j.id not in self.submitted_jobs:
                        submittable_jobs.append(j)
                        self.submitted_jobs.add(j.id)

        if not submittable_jobs:
            return

        # submit the jobs
        submission_output = self.job_submitter.submit(submittable_jobs)

        min_submission_time = None
        for tt, jid, output in submission_output:
            t_ep = tt.timestamp()
            min_submission_time = min( t_ep, min_submission_time ) if min_submission_time else t_ep

        # start the timer if any of the submitted jobs was a "timed" job
        if any([j.is_job_timed for j in submittable_jobs]) and not self.initial_submission_time:
            self.initial_submission_time = min_submission_time

    def print_summary(self):
        """
        Print a simulation summary
        """

        # print TOTAL TIMED simulation time (= T_end_last_timed_job - T_start_first_timed_job)
        if self.initial_submission_time:

            jobid_to_timed_flag = {j.id: j.is_job_timed for j in self.jobs}
            completed_jobs_and_timings = [(ev, time) for (ev, time) in self.event_manager.get_timed_events()
                                          if isinstance(ev, EventComplete)]

            times_of_timed_jobs = [time for (ev, time) in completed_jobs_and_timings
                                   if jobid_to_timed_flag.get(ev.info["job"], False)]

            if times_of_timed_jobs:
                last_timed_msg_timestamp = max(times_of_timed_jobs)
                timed_simulation_time = last_timed_msg_timestamp - self.initial_submission_time

                logger.info("=" * 37)
                logger.info("SIMULATION TIME: {:20.2f}".format(timed_simulation_time))
                logger.info("SIMULATION  T_0: {:20.2f}".format(self.initial_submission_time))
                logger.info("SIMULATION  T_1: {:20.2f}".format(last_timed_msg_timestamp))
                logger.info("=" * 37)
            else:
                logger.warning("INFO: simulation time not available (no timed messaged found..)")

        else:
            logger.info("No timed jobs found.".upper())

    def teardown(self, error=False):
        """
        Various after-run tasks
        :return:
        """

        # first terminates the dispatcher process
        self.event_manager.stop_dispatcher()

        if not error:
            self.print_summary()

        super(ExecutorEventsPar, self).teardown(error)
