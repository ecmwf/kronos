import math
import datetime
import numpy as np


# ///////////////////////////////// DEFINITIONS /////////////////////////////////////////

labels_map = {
    'n_write': "#/sec",
    'kb_write': "Kb/sec",
    'n_read': "#/sec",
    'n_collective': "#/sec",
    'kb_read': "Kb/sec",
    'flops': "#/sec",
    'kb_collective': "Kb/sec",
    'kb_pairwise': "Kb/sec",
    'n_pairwise': "#/sec"
}


list_classes = [
    "main/fc/inigroup",
    "main/fc/ensemble/cf",
    "main/fc/ensemble/pf",
    "main/fc/ensemble/logfiles",
    "main/fc/lag",
]

class_names_complete = [
                        'main/fc/inigroup/serial',
                        'main/fc/inigroup/parallel',

                        'main/fc/ensemble/cf/serial',
                        'main/fc/ensemble/cf/parallel',

                        'main/fc/ensemble/pf/serial',
                        'main/fc/ensemble/pf/parallel',

                        'main/fc/ensemble/logfiles/serial',
                        'main/fc/lag/serial',
                       ]

class_colors = ["b", "r", "g", "m", "c"]


# ///////////////////////////////// UTILITIES /////////////////////////////////////////

def get_class_name(job_name):

    class_name_root = [name for name in list_classes if name in job_name]
    if class_name_root:
        class_name_root_0 = class_name_root[0]

        if "serial" in job_name:
            class_name = class_name_root_0 + "/serial"

        elif "parallel" in job_name:
            class_name = class_name_root_0 + "/parallel"
        else:
            class_name = "unknown tag"

        return class_name
    else:
        raise ValueError("class of job {} not found".format(job_name))


def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970,1,1)).total_seconds()


def cumsum(input_list):
    return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]


def running_series(jobs, times, t0_epoch_wl, class_name_root=None, serial_or_par=None):

    bin_width = times[1] - times[0]

    running = np.zeros(len(times))
    found = 0

    for job in jobs:
        
        if ((class_name_root
                 and serial_or_par
                     and class_name_root in job['label']
                         and serial_or_par in job['label']) or 
           (not class_name_root and not serial_or_par)):
            
            found += 1
            first = int(math.ceil((job["time_start"]-t0_epoch_wl - times[0]) / bin_width))
            last = int(math.floor((job["time_end"]-t0_epoch_wl - times[0]) / bin_width))        
            running[first:last] += 1

    return found, running
