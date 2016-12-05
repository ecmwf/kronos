import numpy as np

"""
This module contains the list of functions usable to fill in an incomplete workload
"""


def step_function(function_config):
    """
    Function that defines a step
    :param function_config:
    :return:
    """

    # x of the step (between 0 and 1)
    x_step = function_config['x_step']

    # default value of x points
    n_values = 6
    eps = 1.0e-6

    # then add two very close points at the step location
    x_values = np.sort(np.append(np.linspace(0, 1, n_values), [x_step, x_step+eps]))
    y_values = np.array([float(cc) for cc in np.sign(x_values-x_step)>0])

    return x_values, y_values


def custom_function(function_config):
    """
    Function that defines a custom distribution of values
    :param function_config:
    :return:
    """

    # x of the step (between 0 and 1)
    x_values = np.array(function_config['x_values'])
    y_values = np.array(function_config['y_values'])

    return x_values, y_values


function_mapping = {
                    'step': step_function,
                    'custom': custom_function,
                   }
