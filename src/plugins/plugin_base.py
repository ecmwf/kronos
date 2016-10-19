class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self):
        self.name = "base"

    def run(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    # def apply_method(self):
        # print "base class: apply_method"
