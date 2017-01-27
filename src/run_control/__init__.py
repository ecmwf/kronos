# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from run_control import controls

runner_list = {
    "pbs": controls.PBSControls,
    "slurm": controls.SLURMControls,
    "local": controls.LocalControls
}


def factory(key, config):

    return runner_list[key](config)
