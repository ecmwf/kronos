class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.name = "base"
        self.config = config

    def run(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")



