
import strict_rfc3339

from datetime import datetime
import os

from kronos_io.json_io_format import JSONIoFormat


class ConfigFormat(JSONIoFormat):
    """
    A standardised format for profiling information.
    """
    format_version = 1
    format_magic = "KRONOS-CONFIG-MAGIC"
    schema_json = os.path.join(os.path.dirname(__file__), "config_schema.json")

    def __init__(self, created=None, uid=None):

        super(ConfigFormat, self).__init__(created=created, uid=uid)

        # # We either initialise from model jobs, or from processed json data
        # assert (model_jobs is not None) != (json_jobs is not None)
        # if model_jobs:
        #     self.profiled_jobs = [self.parse_model_job(m) for m in model_jobs]
        # else:
        #     self.profiled_jobs = json_jobs
        #
        # self.workload_tag = workload_tag

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        return cls(
            created=datetime.fromtimestamp(strict_rfc3339.rfc3339_to_timestamp(data['created'])),
            uid=data['uid']
        )

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """
        output_dict = super(ConfigFormat, self).output_dict()
        # output_dict.update({
        #     "profiled_jobs": self.profiled_jobs,
        #     "workload_tag": self.workload_tag
        # })
        return output_dict
