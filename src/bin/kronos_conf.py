#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config.config_format import ConfigFormat


if __name__ == '__main__':

    print ConfigFormat.describe()



