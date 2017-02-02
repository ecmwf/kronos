# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np


def lin_reg(x_in, y_in, alpha=0.01, niter=100):

    """ simple linear regression """
    theta = np.zeros((2,1))
    cost = 0

    for ii in range(niter):
        cost, grad = calc_grad(x_in, y_in, theta)
        theta = theta - alpha * grad

    return cost, theta


def calc_grad(x_in, y_in, theta):

    """ grad for linear regression """
    mm = len(x_in)

    xx = np.hstack((np.ones((len(x_in), 1)), x_in))

    diff_vec = np.dot(xx,theta)-y_in
    cost = 1./(2.*mm) * np.dot(np.transpose(diff_vec), diff_vec)
    grad = 1./mm * np.dot( np.transpose(xx), np.dot(xx,theta)-y_in )

    return cost, grad


# # ////////////// testing only ///////////////////
# if __name__ == '__main__':
#
#     x = np.array(range(10)).reshape((10,1))
#     y = np.array(range(10)).reshape((10,1)) + 0.8*np.random.rand(10,1)
#     cost, theta = lin_reg(x, y)
#
#     plt.figure(10)
#     plt.plot(x, y, 'r+')
#     xx = np.hstack( (np.ones((len(x),1)),x) )
#     plt.plot(x, np.dot(xx,theta) , 'b-')
#     plt.xlabel('x')
#     plt.ylabel('y')
#     plt.show()
#
#     print "theta={}, cost={}".format(theta, cost)
