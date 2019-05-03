import logging

import numpy as np
from kronos_modeller.workload_filling.filling_strategy import FillingStrategy
from kronos_modeller.kronos_exceptions import ConfigurationError
from kronos_modeller.time_signal.time_signal import TimeSignal
from kronos_modeller.workload_filling import filling_functions as fillf

logger = logging.getLogger(__name__)


class StrategyUserDefaults(FillingStrategy):

    """
    Apply user defined defaults to jobs of a
    certain workload type
    """

    required_config_fields = FillingStrategy.required_config_fields + \
        [
            "apply_to",
            "metrics"
        ]

    def _apply(self, config):

        logger.info("applying defaults")

        # get user functions in the configuration (if any)
        user_functions = config.get("user_functions")

        # apply function to all the specified workloads
        for wl in self.workloads:
            if wl.tag in config['apply_to']:
                self.apply_default_metrics(wl, config, user_functions)

    @staticmethod
    def apply_default_metrics(wl_dest, defaults_dict, functions_config):
        """
        Apply passed defaults values of the time series..
        :param defaults_dict:
        :param functions_config:
        :return:
        """
        logger.info('Applying default values on workload: {}'.format(wl_dest.tag))

        metrics_dict = defaults_dict['metrics']
        np.random.seed(0)
        for job in wl_dest.jobs:
            for ts_name in metrics_dict.keys():

                # go-ahead only if the time-series is missing or priority is less that user key
                if not job.timesignals[ts_name] or \
                        job.timesignals[ts_name].priority <= defaults_dict['priority']:
                    substitute = True
                else:
                    substitute = False

                if substitute:

                    # if the entry is a list, generate the random number in [min, max]
                    if isinstance(metrics_dict[ts_name], list):

                        # check on the length of the list (should be 2)
                        if len(metrics_dict[ts_name]) != 2:
                            err = "For metrics {} 2 values are expected for filling operation, " \
                                  "but got {} instead!".format(ts_name, len(metrics_dict[ts_name]))
                            raise ConfigurationError(err)

                        # generate a random number between provided min and max values
                        y_min = metrics_dict[ts_name][0]
                        y_max = metrics_dict[ts_name][1]
                        random_y_value = y_min + np.random.rand() * (y_max - y_min)
                        job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                          xvals=0.,
                                                                          yvals=float(random_y_value),
                                                                          priority=defaults_dict['priority']
                                                                          )
                    elif isinstance(metrics_dict[ts_name], dict):
                        # this entry is specified through a function (name and scaling)

                        if functions_config is None:
                            raise ConfigurationError('user functions required but not found!')

                        # find required function configuration by name
                        ff_config = [ff for ff in functions_config
                                     if ff["name"] == metrics_dict[ts_name]['function']
                                     ]

                        if len(ff_config) > 1:
                            err = "Error: multiple function have been named {}!".format(metrics_dict[ts_name])
                            raise ConfigurationError(err)
                        else:
                            ff_config = ff_config[0]

                        x_vec_norm, y_vec_norm = fillf.function_mapping[ff_config['type']](ff_config)

                        # rescale x and y according to job duration and scaling factor
                        x_vec = x_vec_norm * job.duration
                        y_vec = y_vec_norm * metrics_dict[ts_name]['scaling']

                        job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                          xvals=x_vec,
                                                                          yvals=y_vec,
                                                                          priority=defaults_dict['priority']
                                                                          )
                    else:
                        raise ConfigurationError('fill in "metrics" entry '
                                                 'should be either a list or dictionary')
