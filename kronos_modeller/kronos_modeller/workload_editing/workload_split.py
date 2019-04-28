import logging

from kronos_modeller.kronos_exceptions import ConfigurationError
from kronos_modeller.workload_data import WorkloadData
from kronos_modeller.workload_editing.editing_strategy import EditingStrategy

logger = logging.getLogger(__name__)


class WorkloadSplit(EditingStrategy):

    """
    Split a workload according to name matching
    """

    required_config_fields = EditingStrategy.required_config_fields + \
        [
            'apply_to',
            'create_workload',
            'split_by',
            'keywords_in',
            'keywords_out'
        ]

    # TODO: list of uniq created workloads (and seq counter)
    workload_created = []

    def _apply(self, config, user_functions):

        logger.info("Applying workload splitting..")

        # Apply function to all the specified workloads
        cutout_workloads = []

        for wl_name in config["apply_to"]:

            logger.info("Splitting workload {}".format(config['apply_to']))
            wl = next(wl for wl in self.workloads if wl.tag == wl_name)

            sub_workloads = wl.split_by_keywords(wl, config)

            logger.info("splitting created workload {} with {} jobs".format(
                sub_workloads.tag,
                len(sub_workloads.jobs)))

            # Accumulate cutout worklaods
            cutout_workloads.extend(sub_workloads)

        # extend the workloads with the newly created ones
        self.workloads.extend(cutout_workloads)

    @staticmethod
    def split_by_keywords(workload, split_config_output):

        # Extract configurations for the splitting
        new_wl_name = split_config_output['create_workload']
        split_attr = split_config_output['split_by']
        kw_include = split_config_output['keywords_in']
        kw_exclude = split_config_output['keywords_out']

        sub_wl_jobs = []
        if kw_include and not kw_exclude:
            for j in workload.jobs:
                if getattr(j, split_attr):
                    if all(kw in getattr(j, split_attr) for kw in kw_include):
                        sub_wl_jobs.append(j)

        elif not kw_include and kw_exclude:
            for j in workload.jobs:
                if getattr(j, split_attr):
                    if not any(kw in getattr(j, split_attr) for kw in kw_exclude):
                        sub_wl_jobs.append(j)

        elif kw_include and kw_exclude:
            sub_wl_jobs = [j for j in workload.jobs
                           if all(kw in getattr(j, split_attr) for kw in kw_include) and not
                           any(kw in getattr(j, split_attr) for kw in kw_exclude)
                           ]

            for j in workload.jobs:
                if getattr(j, split_attr):
                    if all(kw in getattr(j, split_attr) for kw in kw_include) and \
                            not any(kw in getattr(j, split_attr) for kw in kw_exclude):
                        sub_wl_jobs.append(j)
        else:
            raise ConfigurationError("either included or excluded keywords are needed for splitting a workload")

        return WorkloadData(jobs=sub_wl_jobs, tag=new_wl_name)
