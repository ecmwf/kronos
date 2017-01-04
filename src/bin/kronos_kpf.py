#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kronos_io.profile_format import ProfileFormat


if __name__ == '__main__':

    print ProfileFormat.describe()
