import os
import stat

from kronos.executor.job_classes.hpc import HPCJob

run_script_template = """#!/bin/sh

# Start generating the
echo "#!/bin/sh" > killjobs
echo -n "qdel " >> killjobs
chmod +x killjobs

SECONDS=0
"""


run_elem_template = """
WAIT=$(({start_delay} - SECONDS))
if [ "${{WAIT}}" -gt "0" ]; then
    sleep $(({start_delay} - SECONDS))
fi
echo "Submitting job {job_num}"
echo -n "$({submit_command} {submit_script}) " >> killjobs
"""


class RemoteHPCJob(HPCJob):

    run_script_template = run_script_template
    run_elem_template = run_elem_template
    run_script = None

    def __init__(self, job_config, executor, path):

        # We haven't yet implemented remote execution properly
        # TODO: Decide if this is something we want.
        if executor.local_tmpdir is not None:
            raise NotImplementedError

        super(RemoteHPCJob, self).__init__(job_config, executor, path)

    def run(self):
        """
        'Run' this job

        This has a flexible meaning, depending on the setup. There can be many strategies here.

          i) Add to list to run
         ii) Run it immediately
        iii) Set an async timer to run it in the future?
        """

        if RemoteHPCJob.run_script is None:
            run_script_path = os.path.join(self.executor.job_dir, "run.sh")
            print "Opening global run script: {}".format(run_script_path)
            RemoteHPCJob.run_script = open(run_script_path, 'w')
            RemoteHPCJob.run_script.write(self.run_script_template)
            os.chmod(run_script_path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

            # TODO further initialisation here...

        RemoteHPCJob.run_script.write(
            self.run_elem_template.format(
                start_delay=self.start_delay,
                submit_command=self.submit_command,
                submit_script=self.submit_script,
                job_num=self.jobno
            )
        )

