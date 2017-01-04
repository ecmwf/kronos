from schema_description import SchemaDescription

import json
import jsonschema


class JSONIoFormat(object):
    """
    A base class for shared functionality between the KFS and KPF files.
    """

    format_version = None
    format_magic = None
    schema_json = None

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
    def describe(cls):
        """
        Output a description of the JSON in human-readable format
        """
        print SchemaDescription.from_schema(cls.schema())
