#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kronos_io.profile_format import ProfileFormat


if __name__ == '__main__':

    print ProfileFormat.describe()
