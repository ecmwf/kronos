from run_control import controls

runner_list = {
    "pbs": controls.PBSControls,
    "slurm": controls.SLURMControls,
    "local": controls.LocalControls
}


def factory(key, config):

    return runner_list[key](config)
