from config.config import Config


class BaseRunner(object):
    """
    Runner base class:
        -- defines a run mode
    """

    def __init__(self, config):

        assert isinstance(config, Config)

        self.config = config
        self.check_config()

    def check_config(self):
        """
        checks and sets default config options
        """
        pass

    def run(self):
        """
        Run the model according to the configuration options
        """
        raise NotImplementedError("Must be implemented in derived class..")

    def plot_results(self):
        """
        Plot desired results
        """
        raise NotImplementedError("Must be implemented in derived class..")
