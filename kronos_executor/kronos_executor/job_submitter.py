# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import logging
import multiprocessing
import subprocess
from datetime import datetime

from kronos_executor.tools import datetime2epochs

logger = logging.getLogger(__name__)


def submit_job_from_args(submission_and_callback_params):
    """
    Helper function to submit a jobs from its submission (and callback) arguments
    (workaround for distributing among processes the non-pickable job classes)
    :return:
    """

    jid = submission_and_callback_params["jid"]
    proc_args = submission_and_callback_params["submission_params"]
    try:
        output = subprocess.check_output(proc_args)
        success = True
    except subprocess.CalledProcessError as e:
        # circumvent https://bugs.python.org/issue9400
        output = (e.returncode, e.cmd, e.output)
        success = False

    submit_job_from_args.t_queue.put( (datetime.now(), jid) )

    # TODO: callback not strictly needed anymore, but it would be nice to retain..
    # Job.submission_callback_static(output, submission_and_callback_params["callback_params"])

    return jid, success, output


def f_init(q):
    """
    just to initialise the submitting function time queue
    :param q:
    :return:
    """
    submit_job_from_args.t_queue = q


class JobSubmitter(object):

    """
    Responsible for submitting jobs according to simulation events and event-dependencies
    """

    def __init__(self, jobs, event_manager, n_submitters=1):

        self.jobs = jobs
        self.event_manager = event_manager
        self.submitted_jobs = []
        self.initial_submission_time = None

        # structure for efficiently finding submittable jobs
        self.deps_to_jobs_tree, self.job_to_deps = self.build_deps_to_job_tree()

        # workers pool
        self.tsub_queue = multiprocessing.Queue()
        self.submitters_pool = multiprocessing.Pool(n_submitters, f_init, [self.tsub_queue])

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

    def submit_eligible_jobs(self, new_events=None):
        """
        Submit the jobs eligible for submission
        :param new_events:
        :return:
        """

        # If there is no valid event inside search for dependency-free jobs
        if not any(new_events):

            # jobs sent for submission
            _submittable_jobs = [j for j in self.jobs if not j.depends and j.id not in self.submitted_jobs]

            # mark them as submitted now (so they won't be re-added)
            self.submitted_jobs.extend([j.id for j in _submittable_jobs])

            if not _submittable_jobs:
                logger.debug("looks like there are no dependency-free jobs to be submitted. let's continue..")
                return None
            else:

                # submit the jobs
                self.do_submit(_submittable_jobs)

        else:  # otherwise process the arrived dependencies properly..

            _submittable_jobs = []
            for new_event in new_events:

                # loop over all the jobs that depend on this event
                jobs_depending_on_this_event = self.deps_to_jobs_tree.get(new_event.get_hashed(), [])
                for j in jobs_depending_on_this_event:

                    # new structure
                    if new_event.get_hashed() in self.job_to_deps[j.id]:
                        self.job_to_deps[j.id].remove(new_event.get_hashed())

                    if not self.job_to_deps[j.id] and j.id not in self.submitted_jobs:
                        _submittable_jobs.append(j)

                        # list of submitted jobs should be updated already here
                        # to prevent that multiple message with the same content
                        # would submit the same job multiple time..
                        self.submitted_jobs.append(j.id)

            self.do_submit(_submittable_jobs)

    def do_submit(self, submittable_jobs):
        """
        Do the submit with the pool of workers
        :param submittable_jobs:
        :return:
        """

        # submit the jobs
        submission_and_callback_params = [j.get_submission_and_callback_params() for j in submittable_jobs]
        submission_output = self.submitters_pool.map(submit_job_from_args, submission_and_callback_params)

        min_submission_time = None
        for jid, success, output in submission_output:
            if not success:
                raise subprocess.CalledProcessError(*output)

            tt_jj = self.tsub_queue.get()
            t_ep = datetime2epochs(tt_jj[0])
            logger.info("[Proc Time: {} (ep: {})] ---> Submitted job: {}".format(tt_jj[0], t_ep, tt_jj[1]))

            min_submission_time = min( t_ep, min_submission_time ) if min_submission_time else t_ep

        # start the timer if any of the submitted jobs was a "timed" job
        if any([j.is_job_timed for j in submittable_jobs]) and not self.initial_submission_time:
            self.initial_submission_time = min_submission_time



















