from plugins.plugin_base import PluginBase


class PluginARCTUR(PluginBase):

    """
    class defining the ecmwf plugin
    """

    def ingest_data(self):
        raise NotImplementedError("ARCTUR plugin not yet implemented")

    def generate_model(self):
        raise NotImplementedError("ARCTUR plugin not yet implemented")

    def run(self):
        raise NotImplementedError("ARCTUR plugin not yet implemented")

    def postprocess(self):
        raise NotImplementedError("ARCTUR plugin not yet implemented")
