# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import multiprocessing


def do_submit_submittable_jobs(submittable_jobs):

    # do the submit now (i.e. with no dependencies)..
    _submitted_jobs = []
    for j in submittable_jobs:
        j.run([], multi_threading=True)
        _submitted_jobs.append(j.id)

    return _submitted_jobs


class JobSubmitter(object):

    """
    Responsible for submitting jobs according to simulation events and event-dependencies
    """

    def __init__(self, jobs, event_manager, n_submitters=1, n_events_per_worker=1):

        self.jobs = jobs
        self.event_manager = event_manager
        self.submitted_jobs = []

        # structure for efficiently finding submittable jobs
        self.deps_to_jobs_tree = self.build_deps_to_job_tree()

        # workers pool
        self.submitters_pool = multiprocessing.Pool(processes=n_submitters)
        self.n_events_per_worker = n_events_per_worker

    def build_deps_to_job_tree(self):
        """
        Build a structure that allows getting jobs that depend on a particular event
        :return:
        """

        _deps_2_jobs = {}
        for j in self.jobs:
            for d in j.depends:
                _deps_2_jobs.setdefault(d.get_hashed(), []).append(j)

        return _deps_2_jobs

    def submit_eligible_jobs(self, new_events=None):
        """
        Submit the jobs eligible for submission
        :param new_events:
        :return:
        """

        # make sure it is a list of events (might be a list of just one..)
        if not isinstance(new_events, list):
            new_events = [new_events]

        # now make sure that there is at least a valid event inside..
        if not any(new_events):

            _submittable_jobs = [j for j in self.jobs if not j.depends and j.id not in self.submitted_jobs]

            # submit the jobs
            self.submitted_jobs.extend(do_submit_submittable_jobs(_submittable_jobs))
            # self.submitted_jobs.extend(self.submitters_pool.map(do_submit_submittable_jobs, _submittable_jobs))

        else:

            _submittable_jobs = []
            for new_event in new_events:

                # print "processing event ", new_event

                # loop over all the jobs that depend on this event
                jobs_depending_on_this_event = self.deps_to_jobs_tree.get(new_event.get_hashed(), [])
                for j in jobs_depending_on_this_event:

                    # if this dependency is still in the job list of dependencies, remove it
                    if new_event in j.depends:
                        j.depends.remove(new_event)

                    # if there are no dependencies left at this point, this job is ready for submission..
                    if not j.depends and j.id not in self.submitted_jobs:
                        _submittable_jobs.append(j)

            # submit the jobs
            self.submitted_jobs.extend(do_submit_submittable_jobs(_submittable_jobs))
            # self.submitted_jobs.extend(self.submitters_pool.map(do_submit_submittable_jobs, _submittable_jobs))



















