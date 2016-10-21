from plugins.plugin_base import PluginBase


class PluginEPCC(PluginBase):

    """
    class defining the ecmwf plugin
    """

    def ingest_data(self):
        raise NotImplementedError("EPCC plugin not yet implemented")

    def generate_model(self):
        raise NotImplementedError("EPCC plugin not yet implemented")

    def run(self):
        raise NotImplementedError("EPCC plugin not yet implemented")

    def postprocess(self):
        raise NotImplementedError("EPCC plugin not yet implemented")