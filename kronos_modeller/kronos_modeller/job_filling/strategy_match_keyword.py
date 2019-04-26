import logging

from kronos_modeller.job_filling.strategy_base import StrategyBase
from kronos_modeller.kronos_exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class StrategyMatchKeyword(StrategyBase):

    """
    Apply job metrics from a name matching job
    """

    required_config_fields = StrategyBase.required_config_fields + \
        [
            'keywords',
            'similarity_threshold',
            'source_workloads',
            'apply_to'
        ]

    def _apply(self, config, user_functions):

        # Apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0

        for wl_source_tag in config['source_workloads']:

            try:
                wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)
            except StopIteration:
                raise ConfigurationError("Source Workload {} not found".format(wl_source_tag))

            for wl_dest_tag in config['apply_to']:

                try:
                    wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                except StopIteration:
                    raise ConfigurationError("Destination Workload {} not found".format(wl_dest_tag))

                n_destination_jobs += len(wl_dest.jobs)

                n_job_matched += wl_dest.apply_lookup_table(wl_source,
                                                            config['similarity_threshold'],
                                                            config['priority'],
                                                            config['keywords'],
                                                            )

        logger.info("jobs matched/destination jobs = [{}/{}]".format(n_job_matched, n_destination_jobs))


