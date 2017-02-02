from kronos.executor.job_classes.pbs import PBSMixin
from kronos.executor.job_classes.remote_hpc import RemoteHPCJob


class Job(PBSMixin, RemoteHPCJob):
    """
    This job creates PBS scripts, and a shell script that can launch them
    """
    pass

