# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import numpy as np


def r_gyration(data_matrix):
    """
    Calculate the radius of gyration of a set of points (each row of the matrix represents a point
    in a multi-dimensional space)
    :param data_matrix:
    :return:
    """

    rad_gyr = 0.0

    matrix_mean = np.mean(data_matrix, axis=0)

    if data_matrix.shape[0] == 1:

        # if there is only one element in the matrix, returns 0
        return 0.0
    else:

        for row in data_matrix:
            rad2 = np.linalg.norm(row-matrix_mean)**2
            rad_gyr += rad2

        rad_gyr /= float(data_matrix.shape[0])

        return np.sqrt(rad_gyr)

