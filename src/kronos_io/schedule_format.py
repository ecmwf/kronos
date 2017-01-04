import os

from kronos_io.json_io_format import JSONIoFormat


class ScheduleFormat(JSONIoFormat):
    """
    A standardised format for profiling information.
    """
    format_version = 1
    format_magic = "KRONOS-KSF-MAGIC"
    schema_json = os.path.join(os.path.dirname(__file__), "schedule_schema.json")

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        raise NotImplementedError

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """
        output_dict = super(ScheduleFormat, self).output_dict()
        raise NotImplementedError
        return output_dict

