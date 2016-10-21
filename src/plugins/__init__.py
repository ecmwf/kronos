from ecmwf import plugin_ecmwf
from arctur import plugin_arctur
from epcc import plugin_epcc

plugins_list = {
    "ecmwf": plugin_ecmwf.PluginECMWF,
    "arctur": plugin_arctur.PluginARCTUR,
    "epcc": plugin_epcc.PluginEPCC,
}


def factory(key, config):

    return plugins_list[key](config)
