# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import sys

def print_colour(col, text, end="\n", flush=False):
    """
    Print to the terminal in the specified colour
    """

    colour_map = {
        "black": "0;30",
        "red": "0;31",
        "green": "0;32",
        "brown": "0;33",
        "orange": "0;33",
        "blue": "0;34",
        "purple": "0;35",
        "cyan": "0;36",
        "light grey": "0;37",
        "dark grey": "1;30",
        "light red": "1;31",
        "light green": "1;32",
        "yellow": "1;33",
        "light blue": "1;34",
        "light purple": "1;35",
        "light cyan": "1;36",
        "white": "1;37",
    }

    colour_str = "\033[{}m".format(colour_map.get(col, "0"))
    reset_str = "\033[0m"

    sys.stdout.write("{}{}{}{}".format(colour_str, text, reset_str, end))
    if flush:
        sys.stdout.flush()
