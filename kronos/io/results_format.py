# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import os
from datetime import datetime

import strict_rfc3339

from kronos.io.json_io_format import JSONIoFormat


class ResultsFormat(JSONIoFormat):
    """
    A standardised format for profiling information.
    """
    format_version = 1
    format_magic = "KRONOS-KRF-MAGIC"
    schema_json = os.path.join(os.path.dirname(__file__), "results_schema.json")

    def __init__(self, ranks=None, created=None, uid=None):

        super(ResultsFormat, self).__init__(created=created, uid=uid)

        # n.b. We currently have no means to initialise directly from python model. Only for
        #      READING files produced by the coordinator.
        assert(ranks is not None)

        self.ranks = ranks
        self.uid = uid
        # self.time_series = time_series

    def __eq__(self, other):
        """
        Are two profiles the same?
        """
        # We don't test the UID/created timestamp. Not interesting. Only care about the data.

        if len(self.ranks) != len(other.statistics):
            return False

        for j1, j2 in zip(self.ranks, other.statistics):
            if j1 != j2:
                return False

        return True

    def __ne__(self, other):
        return not (self == other)

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        return cls(
            ranks=data['ranks'],
            created=datetime.fromtimestamp(strict_rfc3339.rfc3339_to_timestamp(data['created'])),
            uid=data['uid']
        )

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """

        output_dict = {
            "version": self.format_version,
            "tag": self.format_magic,
            "created": self.created,
            "uid": self.uid,
            "ranks": self.ranks,
        }

        return output_dict
