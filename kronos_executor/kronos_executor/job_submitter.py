
import datetime
import itertools
import logging
import multiprocessing
import subprocess

logger = logging.getLogger(__name__)


class JobException(Exception):
    """Exception raised when a job submission fails"""
    def __init__(self, jid, exc):
        self.jid = jid
        self.exc = exc

    def __str__(self):
        return "job {}: {!s}".format(self.jid, self.exc)


def _submit_job(jid, cmdline):
    """Submit the job with the given id and command line
    :param jid: job id
    :param cmdline: command line (list of str)
    :return: submission timestamp (datetime), job id, command output (bytes)
    :raise: JobException in case the submission fails"""
    try:
        output = subprocess.check_output(cmdline)
        t_finish = datetime.datetime.now()
        return t_finish, jid, output
    except Exception as e:
        raise JobException(jid, e)


class JobSubmitter:
    """Submit jobs in parallel
    :param n_submitters: number of submitter processes"""

    def __init__(self, n_submitters):
        self._submitters = multiprocessing.Pool(n_submitters)

    def submit(self, jobs, deps=None):
        """Submit the given jobs
        Upon successful submission, the job's `submission_callback` method is
        called in a separate thread with the output of the submission command.
        :param jobs: list of Job objects to submit
        :param deps: (optional) dependencies for each job
        :return: list of (timestamp, job id, output) tuples"""
        if deps is None:
            deps = itertools.repeat([])
        futures = []
        for job, jdeps in zip(jobs, deps):
            job_args = (job.id, job.get_submission_arguments(jdeps))
            futures.append(
                self._submitters.apply_async(
                    _submit_job, job_args,
                    callback=self._wrap_callback(job)))

        results = []
        for future in futures:
            tt, jid, output = future.get()
            t_ep = tt.timestamp()
            logger.info("[Proc Time: {} (ep: {})] ---> Submitted job: {}".format(tt, t_ep, jid))
            results.append((tt, jid, output))

        return results

    def close(self):
        self._submitters.close()
        self._submitters.join()

    def _wrap_callback(self, job):
        def callback(res):
            t_finish, jid, output = res
            job.submission_callback(output)
        return callback
