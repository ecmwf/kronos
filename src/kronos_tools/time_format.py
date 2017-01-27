# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


def format_seconds(s):

    # Round to a whole number of seconds
    s = int(round(s))

    mins, secs = divmod(s, 60)
    out = "{}s".format(secs)

    if mins > 0:
        hrs, mins = divmod(mins, 60)
        out = "{}m {}".format(mins, out)

        if hrs > 0:
            days, hrs = divmod(hrs, 24)
            out = "{}h {}".format(hrs, out)

            if days > 0:
                out = "{}d {}".format(days, out)

    return out


