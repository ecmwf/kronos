# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from .schema_description import SchemaDescription

import os
import json
import jsonschema
import strict_rfc3339


class JSONIoFormat(object):
    """
    A base class for shared functionality between the KFS and KProfile files.
    """

    format_version = None
    format_magic = None
    schema_json = None

    def __init__(self, created=None, uid=None):
        self.created = created
        self.uid = uid

    def __str__(self):
        return "{}(njobs={})".format(self.__class__, len(self.profiled_jobs))

    def __bytes__(self):
        return str(self).encode('utf-8')

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
        return SchemaDescription.from_schema(cls.schema())

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

    def write(self, f, indent=None):
        """
        Exports the data in the required format.
        """
        output_dict = self.output_dict()

        self.validate_json(output_dict)

        json.dump(output_dict, f, indent=indent)

    def write_filename(self, filename, indent=None):
        with open(filename, 'w') as f:
            self.write(f, indent=indent)

    @classmethod
    def from_file(cls, f, validate_json=True):
        """
        Given a KProfile file, load it and make the data appropriately available
        """
        data = json.load(f)

        if validate_json:
            cls.validate_json(data)

        return cls.from_json_data(data)

    @classmethod
    def from_filename(cls, filename, validate_json=True):
        with open(filename, 'r') as f:
            return cls.from_file(f, validate_json)


