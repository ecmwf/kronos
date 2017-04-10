# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest
import numpy as np

from kronos.core.kronos_tools.gyration_radius import r_gyration


class RGyrTest(unittest.TestCase):

    def test_min_not_none(self):

        # points equi-distant from center
        rad = 66.6

        ang = [aa * 2 * np.pi / 100.0 for aa in np.arange(0, 100)]
        xv = rad*np.cos(ang)
        yv = rad*np.sin(ang)
        mm = np.hstack((xv[:, np.newaxis], yv[:, np.newaxis]))
        self.assertAlmostEqual(r_gyration(mm), rad)


if __name__ == "__main__":
    unittest.main()
