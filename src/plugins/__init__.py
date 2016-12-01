from ecmwf import plugin_ecmwf
from user_workload import plugin_user

plugins_list = {
    "ecmwf": plugin_ecmwf.PluginECMWF,
    "user": plugin_user.PluginUSER,
}


def factory(key, config):

    return plugins_list[key](config)
