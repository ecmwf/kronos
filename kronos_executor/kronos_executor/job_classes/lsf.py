from kronos_executor.synapp_job import SyntheticAppJob


class LSFMixin:
    """
    Define the templates for LSF
    """

    allinea_launcher_command = "map --profile aprun"


class Job(LSFMixin, SyntheticAppJob):
    pass

