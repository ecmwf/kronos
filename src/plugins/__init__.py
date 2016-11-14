from ecmwf import plugin_ecmwf
from arctur import plugin_arctur
from epcc import plugin_epcc
from user_workload import plugin_user

plugins_list = {
    "ecmwf": plugin_ecmwf.PluginECMWF,
    "arctur": plugin_arctur.PluginARCTUR,
    "epcc": plugin_epcc.PluginEPCC,
    "user": plugin_user.PluginUSER,
}


def factory(key, config):

    return plugins_list[key](config)
