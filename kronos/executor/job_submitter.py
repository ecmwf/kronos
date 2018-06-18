# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import multiprocessing
import subprocess

from kronos.executor.job_classes.trivial_job_echo import Job


def submit_job_from_args(submission_and_callback_params):
    """
    Helper function to submit a jobs from its submission (and callback) arguments
    (workaround for distributing among processes the non-pickable job classes)
    :param jid_and_subprocess_args:
    :return:
    """

    jid = submission_and_callback_params["jid"]
    proc_args = submission_and_callback_params["submission_params"]

    output = subprocess.check_output(proc_args)

    # submission_callback(output)
    Job.submission_callback_static(output, submission_and_callback_params["callback_params"])

    return jid, output


class JobSubmitter(object):

    """
    Responsible for submitting jobs according to simulation events and event-dependencies
    """

    def __init__(self, jobs, event_manager, n_submitters=1):

        self.jobs = jobs
        self.event_manager = event_manager
        self.submitted_jobs = []

        # structure for efficiently finding submittable jobs
        self.deps_to_jobs_tree, self.job_to_deps = self.build_deps_to_job_tree()

        # workers pool
        self.submitters_pool = multiprocessing.Pool(processes=n_submitters)

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

        # # write this structure to a file
        # if os.path.exists("log_events.log"): os.remove("log_events.log")
        # with open("log_events.log", "w") as myfile:
        #     for k,v in _deps_2_jobs.iteritems():
        #         myfile.write( "complete_job-{}: {}\n".format(k[2][1], ",".join([str(j.id) for j in v])) )

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

            _submittable_jobs = [j for j in self.jobs if not j.depends and j.id not in self.submitted_jobs]

            if not _submittable_jobs:
                print "WARNING: looks like there are no dependency-free jobs to be submitted. let's continue.."
                return None
            else:

                # submit the jobs
                self.do_submit(_submittable_jobs)

        else:  # otherwise process the arrived dependencies properly..

            _submittable_jobs = []
            for new_event in new_events:

                # print "checking deps for event {}..".format(new_event)

                # loop over all the jobs that depend on this event
                jobs_depending_on_this_event = self.deps_to_jobs_tree.get(new_event.get_hashed(), [])
                for j in jobs_depending_on_this_event:

                    # print "--job:{}".format(j.id)

                    # # if this dependency is still in the job list of dependencies, remove it
                    # if new_event in j.depends:
                    #     j.depends.remove(new_event)
                    #
                    # # if there are no dependencies left at this point, this job is ready for submission..
                    # if not j.depends and j.id not in self.submitted_jobs:
                    #     _submittable_jobs.append(j)

                    # new structure
                    if new_event.get_hashed() in self.job_to_deps[j.id]:
                        self.job_to_deps[j.id].remove(new_event.get_hashed())

                    if not self.job_to_deps[j.id] and j.id not in self.submitted_jobs:
                        _submittable_jobs.append(j)

            # print "..deps all checked!"
            self.do_submit(_submittable_jobs)

    def do_submit(self, submittable_jobs):
        """
        Do the submit with the pool of workers
        :param submittable_jobs:
        :return:
        """

        # submit the jobs
        # print "submitting jobs.."
        submission_and_callback_params = [j.get_submission_and_callback_params() for j in submittable_jobs]
        submission_output = self.submitters_pool.map(submit_job_from_args, submission_and_callback_params)
        print "\n".join("Submitted job: {}".format(out[0]) for out in submission_output)
        self.submitted_jobs.extend([j.id for j in submittable_jobs])
        # print "..jobs all submitted!"

        # # write this structure to a file
        # with open("log_events.log", "a") as myfile:
        #     myfile.write("\n"+"\n".join("Submitted job: {}".format(out[0]) for out in submission_output))



















