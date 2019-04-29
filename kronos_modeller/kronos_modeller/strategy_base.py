from kronos_modeller.kronos_exceptions import ConfigurationError


class StrategyBase(object):

    """
    Base class that represents a job filling strategy
    """

    # required params
    required_config_fields = []

    def __init__(self, workloads):
        """
        Store all the job workloads
        :param workloads:
        """
        self.workloads = workloads

    def check_config(self, config):
        """
        make sure that all the required params
        are set in the config
        :return:
        """
        # check that all the required fields are set
        for req_item in self.required_config_fields:
            if req_item not in config.keys():
                err = "{} requires config {}".format(self.__class__.__name__, req_item)
                raise ConfigurationError(err)

    def apply(self, config):
        """
        Main interface to apply the strategy
        :param config: strategy configuration
        :return:
        """

        # check the config
        self.check_config(config)

        # apply the strategy
        self._apply(config)

    def _apply(self, config):
        """
        Specialised apply (needs to be implemented
        by the child classes)
        :param config: strategy configuration
        :return:
        """

        raise NotImplementedError

