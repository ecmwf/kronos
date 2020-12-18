
import os

from kronos_executor.hpc import HPCJob


def job_iterator(jobs):
    for job in jobs:
        assert isinstance(job, dict)
        job_repeats = job.get("repeat", 1)
        for i in range(job_repeats):
            yield job


class CompositeJob(HPCJob):

    default_template = "composite.sh"

    def __init__(self, job_config, executor, path):
        self.jobs = self.load_jobs(job_config, executor, path)
        super(CompositeJob, self).__init__(job_config, executor, path)
        self.needs_read_cache = any(j.needs_read_cache for j in self.jobs)

    def load_jobs(self, config, executor, path):
        jobs = []
        for job_num, job_config in enumerate(job_iterator(config.get('jobs', []))):
            job_dir = os.path.join(path, "job-{}".format(job_num))
            job_config['job_num'] = "{}.{}".format(config['job_num'], job_num)

            job_class_name = job_config.setdefault("job_class", "synapp")

            j = executor.load_job(job_class_name, job_config, job_dir)
            self.needs_read_cache = self.needs_read_cache or j.needs_read_cache
            jobs.append(j)
        return jobs

    def build_dependencies(self):
        deps = super(CompositeJob, self).build_dependencies()
        for j in self.jobs:
            deps.extend(j.depends)
        return deps

    def customised_generated_internals(self, script_format):
        script_format['jobs'] = self.jobs
        for j in self.jobs:
            j.generate()

Job = CompositeJob
