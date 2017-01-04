from schema_description import SchemaDescription

import os
import json
import jsonschema
import strict_rfc3339


class JSONIoFormat(object):
    """
    A base class for shared functionality between the KFS and KPF files.
    """

    format_version = None
    format_magic = None
    schema_json = None

    def __init__(self, created=None, uid=None):
        self.created = created
        self.uid = uid

    def __unicode__(self):
        return "{}(njobs={})".format(self.__class__, len(self.profiled_jobs))

    def __str__(self):
        return unicode(self).encode('utf-8')

    @classmethod
    def schema(cls):
        """
        Obtain the json schema for the given format
        """
        assert cls.format_version is not None
        assert cls.format_magic is not None
        assert cls.schema_json is not None

        with open(cls.schema_json, 'r') as fschema:
            str_schema = fschema.read() % {
                "kronos-version": cls.format_version,
                "kronos-magic": cls.format_magic
            }
            return json.loads(str_schema)

    @classmethod
    def validate_json(cls, js):
        """
        Do validation of a dictionary that has been loaded from (or will be written to) a JSON
        """
        jsonschema.validate(js, cls.schema(), format_checker=jsonschema.FormatChecker())

    @classmethod
    def from_json_data(cls, data):
        raise NotImplementedError

    @classmethod
    def describe(cls):
        """
        Output a description of the JSON in human-readable format
        """
        print SchemaDescription.from_schema(cls.schema())

    def output_dict(self):
        """
        Obtain the (base components of) the output data. This routine should be
        overloaded and extended for real formats.
        """
        return {
            "version": self.format_version,
            "tag": self.format_magic,
            "created": strict_rfc3339.now_to_rfc3339_utcoffset(),
            "uid": os.getuid(),
        }

    def write(self, f):
        """
        Exports the data in the required format.
        """
        output_dict = self.output_dict()

        self.validate_json(output_dict)

        json.dump(output_dict, f)

    def write_filename(self, filename):
        with open(filename, 'w') as f:
            self.write(f)

    @classmethod
    def from_file(cls, f):
        """
        Given a KPF file, load it and make the data appropriately available
        """
        data = json.load(f)
        cls.validate_json(data)

        return cls.from_json_data(data)

    @classmethod
    def from_filename(cls, filename):
        with open(filename, 'r') as f:
            return cls.from_file(f)


