class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.config = config

    def ingest_data(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def generate_model(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def run(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def postprocess(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")



